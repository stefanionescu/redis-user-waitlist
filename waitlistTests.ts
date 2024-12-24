import dotenv from 'dotenv';
import { WaitlistManager } from './waitlistManager';

dotenv.config();

// Global waitlist instance
let waitlist: WaitlistManager;

const GREEN_CHECK = '\x1b[32m✓\x1b[0m';
const RED_X = '\x1b[31m✗\x1b[0m';

async function clearRedis(): Promise<void> {
  const redis = (waitlist as any).redis;
  await redis.flushdb();
}

async function forceCleanup(): Promise<void> {
  try {
    if (waitlist) {
      await clearRedis();
      await waitlist.disconnect();
    }
  } catch (error) {
    console.error('Failed to force cleanup:', error);
  }
}

async function runTest(name: string, testFn: () => Promise<void>) {
  try {
    await testFn();
    console.log(`${GREEN_CHECK} ${name}`);
  } catch (err) {
    console.log(`${RED_X} ${name}`);
    // Handle the error type properly
    const error = err as Error;
    console.error('\x1b[31m', error.message || 'Unknown error', '\x1b[0m');
    throw error;  // Re-throw to stop test suite
  }
}

async function testConcurrentEmailUsers(): Promise<void> {
  const operations = Array.from({ length: 20 }, (_, i) => {
    return waitlist.insertUser(`user${i}`, {
      email: `user${i}@test.com`,
      metadata: { name: `User ${i}` }
    });
  });

  const results = await Promise.all(operations);
  const positions = results.map(r => r.position);
  const alreadyExisted = results.map(r => r.already_existed);
  const uniquePositions = new Set(positions);

  if (uniquePositions.size !== 20) {
    throw new Error(`Expected 20 unique positions, got ${uniquePositions.size}`);
  }
  if (Math.min(...positions) !== 1 || Math.max(...positions) !== 20) {
    throw new Error(`Position range incorrect: ${positions.join(', ')}`);
  }
  if (!alreadyExisted.every(existed => !existed)) {
    throw new Error('Some users were reported as duplicates');
  }

  const finalPositions = await Promise.all(
    Array.from({ length: 20 }, (_, i) => waitlist.getPosition(`user${i}`))
  );
  if (!finalPositions.every(p => p > 0 && p <= 20)) {
    throw new Error(`Final positions invalid: ${finalPositions.join(', ')}`);
  }
}

async function testConcurrentDuplicateUserEmail(): Promise<void> {
  const operations = Array.from({ length: 10 }, () => {
    return waitlist.insertUser('duplicate_user', {
      email: 'duplicate@test.com',
      metadata: { name: 'Duplicate User' }
    });
  });

  const results = await Promise.all(operations);
  const position = results[0].position;
  const allSamePosition = results.every(r => r.position === position);
  const newInsertions = results.filter(r => !r.already_existed);

  if (newInsertions.length !== 1) {
    throw new Error('Expected only one new insertion');
  }
  if (!allSamePosition) {
    throw new Error('All operations did not return the same position');
  }

  const finalPosition = await waitlist.getPosition('duplicate_user');
  if (finalPosition !== position) {
    throw new Error(`Final position does not match: ${finalPosition} !== ${position}`);
  }
}

async function testOrderedBumpUp(): Promise<void> {
  // Insert 20 users sequentially
  for (let i = 0; i < 20; i++) {
    await waitlist.insertUser(`user${i}`, {
      email: `user${i}@test.com`,
      metadata: { name: `User ${i}` }
    });
  }

  // Move user 8 to position 4
  await waitlist.bumpUserUp('user8', 4);
  
  // Get actual order
  const actualOrder = await waitlist._getOrderedIds();

  // Verify user8's new position
  const user8Position = await waitlist.getPosition('user8');
  if (user8Position !== 4) {
    throw new Error(`Expected user8 to be at position 4, but found at position ${user8Position}`);
  }

  // Verify the exact order of first 5 positions
  const expectedStart = ['user0', 'user1', 'user2', 'user8', 'user3'];
  const actualStart = actualOrder.slice(0, 5);
  
  if (JSON.stringify(actualStart) !== JSON.stringify(expectedStart)) {
    throw new Error(
      `Expected order: ${expectedStart.join(', ')}\n` +
      `Actual order: ${actualStart.join(', ')}`
    );
  }

  // Verify email mappings are correct
  for (const userId of actualStart) {
    const email = `${userId}@test.com`;
    const mappedId = await waitlist._getEmailMapping(email);
    if (mappedId !== userId) {
      throw new Error(`Email mapping incorrect for ${userId}. Expected ${userId}, got ${mappedId}`);
    }
  }
}

async function testLargeScaleBumpUp(): Promise<void> {
  // First verify Redis is clean
  const initialCount = await waitlist._getOrderedIds();
  if (initialCount.length !== 0) {
    throw new Error(`Redis not clean at start. Found ${initialCount.length} entries`);
  }

  // Insert 2000 users
  const insertOperations = Array.from({ length: 2000 }, (_, i) => {
    return waitlist.insertUser(`mass_user${i}`, {
      email: `mass_user${i}@test.com`,
      metadata: { name: `Mass User ${i}` }
    });
  });

  await Promise.all(insertOperations);

  // Verify initial count
  const afterInsertCount = await waitlist._getOrderedIds();
  if (afterInsertCount.length !== 2000) {
    throw new Error(`Wrong number of users after insert. Expected 2000, got ${afterInsertCount.length}`);
  }

  // Move users 500-1000 to positions 400-900
  for (let i = 499; i < 1000; i++) {
    await waitlist.bumpUserUp(`mass_user${i}`, i - 99);
  }

  // Get final count and order
  const finalOrder = await waitlist._getOrderedIds();
  if (finalOrder.length !== 2000) {
    console.error('Users found:', finalOrder.map(id => id).join(', '));
    throw new Error(`Expected 2000 users, found ${finalOrder.length}`);
  }

  // Verify no duplicate IDs
  const uniqueIds = new Set(finalOrder);
  if (uniqueIds.size !== 2000) {
    throw new Error(`Found ${finalOrder.length} total entries but only ${uniqueIds.size} unique IDs`);
  }

  // Verify all expected users exist
  for (let i = 0; i < 2000; i++) {
    if (!uniqueIds.has(`mass_user${i}`)) {
      throw new Error(`Missing user: mass_user${i}`);
    }
  }
}

async function testRepeatedBumpUp(): Promise<void> {
  // Insert 20 users sequentially
  for (let i = 0; i < 20; i++) {
    await waitlist.insertUser(`bump_user${i}`, {
      email: `bump_user${i}@test.com`,
      metadata: { name: `Bump User ${i}` }
    });
  }

  // Move users to their final positions in reverse order
  for (let i = 0; i < 20; i++) {
    const userToMove = 19 - i;
    const targetPos = i + 1;
    await waitlist.bumpUserUp(`bump_user${userToMove}`, targetPos);
  }

  // Get final order
  const finalOrder = await waitlist._getOrderedIds();

  // Verify exact final order (19 down to 0)
  const expectedOrder = Array.from({ length: 20 }, (_, i) => `bump_user${19 - i}`);
  if (JSON.stringify(finalOrder) !== JSON.stringify(expectedOrder)) {
    throw new Error(
      'Expected reversed order: 19,18,17,...,2,1,0\n' +
      `Got: ${finalOrder.map(id => parseInt(id.replace('bump_user', ''))).join(',')}`
    );
  }

  // Verify email mappings match reversed order
  for (let i = 0; i < finalOrder.length; i++) {
    const userId = finalOrder[i];
    const email = `${userId}@test.com`;
    const mappedId = await waitlist._getEmailMapping(email);
    if (mappedId !== userId || mappedId !== `bump_user${19 - i}`) {
      throw new Error(`Email mapping at position ${i + 1} incorrect. Expected bump_user${19 - i}, got ${mappedId}`);
    }
  }
}

async function testStepByStepBumpUp(): Promise<void> {
  // Insert 20 users sequentially
  for (let i = 0; i < 20; i++) {
    await waitlist.insertUser(`bump_user${i}`, {
      email: `bump_user${i}@test.com`,
      metadata: { name: `Bump User ${i}` }
    });
  }

  // Move user19 up one position at a time
  for (let targetPos = 19; targetPos >= 1; targetPos--) {
    await waitlist.bumpUserUp('bump_user19', targetPos);
    const currentOrder = await waitlist._getOrderedIds();

    // Verify position after each move
    const currentPosition = await waitlist.getPosition('bump_user19');
    if (currentPosition !== targetPos) {
      throw new Error(`Expected user19 at position ${targetPos}, but found at ${currentPosition}`);
    }

    // Verify the array is correct up to this point
    const expectedOrder = Array.from({ length: 20 }, (_, i) => {
      if (i < targetPos - 1) return `bump_user${i}`;
      if (i === targetPos - 1) return 'bump_user19';
      if (i <= 18) return `bump_user${i-1}`;
      return `bump_user18`;
    });

    if (JSON.stringify(currentOrder) !== JSON.stringify(expectedOrder)) {
      throw new Error(
        `At target position ${targetPos}:\n` +
        `Expected: ${expectedOrder.map(id => parseInt(id.replace('bump_user', ''))).join(',')}\n` +
        `Got: ${currentOrder.map(id => parseInt(id.replace('bump_user', ''))).join(',')}`
      );
    }
  }

  // Verify final state
  const finalOrder = await waitlist._getOrderedIds();

  // Final position should be 1 (1-based)
  const finalPosition = await waitlist.getPosition('bump_user19');
  if (finalPosition !== 1) {
    throw new Error(`Expected user19 at position 1, but found at ${finalPosition}`);
  }

  // Expected final order: [19,0,1,2,3,...,18]
  const expectedFinalOrder = ['bump_user19'].concat(
    Array.from({ length: 19 }, (_, i) => `bump_user${i}`)
  );
  
  if (JSON.stringify(finalOrder) !== JSON.stringify(expectedFinalOrder)) {
    throw new Error(
      'Expected final order: 19,0,1,2,3,...,18\n' +
      `Got: ${finalOrder.map(id => parseInt(id.replace('bump_user', ''))).join(',')}`
    );
  }

  // Verify email mappings match final order [19,0,1,2,3,...,18]
  for (let i = 0; i < finalOrder.length; i++) {
    const userId = finalOrder[i];
    const email = `${userId}@test.com`;
    const mappedId = await waitlist._getEmailMapping(email);
    if (mappedId !== userId || mappedId !== finalOrder[i]) {
      throw new Error(`Email mapping at position ${i + 1} incorrect. Expected ${userId}, got ${mappedId}`);
    }
  }
}

async function testConcurrentBumpToSamePosition(): Promise<void> {
  // Insert 20 users sequentially
  for (let i = 0; i < 20; i++) {
    await waitlist.insertUser(`bump_user${i}`, {
      email: `bump_user${i}@test.com`,
      metadata: { name: `Bump User ${i}` }
    });
  }

  // Concurrently move users 16-19 to position 2
  await Promise.all([
    waitlist.bumpUserUp('bump_user16', 2),
    waitlist.bumpUserUp('bump_user17', 2),
    waitlist.bumpUserUp('bump_user18', 2),
    waitlist.bumpUserUp('bump_user19', 2)
  ]);

  // Get final order
  const finalOrder = await waitlist._getOrderedIds();

  // Verify the structure:
  // 1. First position should be 0
  if (finalOrder[0] !== 'bump_user0') {
    throw new Error('First position should be user0');
  }

  // 2. Next four positions should contain users 16-19 in reverse order
  const movedUsers = finalOrder.slice(1, 5).map(id => parseInt(id.replace('bump_user', '')));
  const expectedMovedUsers = [19, 18, 17, 16];
  if (JSON.stringify(movedUsers) !== JSON.stringify(expectedMovedUsers)) {
    throw new Error(
      'Positions 2-5 should contain users 19,18,17,16 in that order\n' +
      `Got: ${movedUsers.join(',')}`
    );
  }

  // 3. Remaining positions should be 1-15 in order
  const remainingUsers = finalOrder.slice(5).map(id => parseInt(id.replace('bump_user', '')));
  const expectedRemaining = Array.from({ length: 15 }, (_, i) => i + 1);
  if (JSON.stringify(remainingUsers) !== JSON.stringify(expectedRemaining)) {
    throw new Error(
      'Remaining positions should be users 1-15 in order\n' +
      `Expected: ${expectedRemaining.join(',')}\n` +
      `Got: ${remainingUsers.join(',')}`
    );
  }

  // Verify email mappings match final order [0,19,18,17,16,1,2,3,...,15]
  for (let i = 0; i < finalOrder.length; i++) {
    const userId = finalOrder[i];
    const email = `${userId}@test.com`;
    const mappedId = await waitlist._getEmailMapping(email);
    if (mappedId !== userId || mappedId !== finalOrder[i]) {
      throw new Error(`Email mapping at position ${i + 1} incorrect. Expected ${userId}, got ${mappedId}`);
    }
  }
}

async function testPhoneAttachment(): Promise<void> {
  // Insert 20 users with emails only
  for (let i = 0; i < 20; i++) {
    await waitlist.insertUser(`user${i}`, {
      email: `user${i}@test.com`,
      metadata: { name: `User ${i}` }
    });
  }

  // Attach phone numbers to all users
  const attachResults = await Promise.all(
    Array.from({ length: 20 }, (_, i) => 
      waitlist.attachPhone(`user${i}`, `+1555000${i.toString().padStart(4, '0')}`)
    )
  );

  if (!attachResults.every(result => result === true)) {
    throw new Error('Not all phone attachments succeeded');
  }

  // Verify phone mappings
  for (let i = 0; i < 20; i++) {
    const userId = `user${i}`;
    const phone = `+1555000${i.toString().padStart(4, '0')}`;
    const mappedId = await waitlist._getPhoneMapping(phone);
    
    if (mappedId !== userId) {
      throw new Error(`Phone mapping incorrect for ${userId}. Expected ${userId}, got ${mappedId}`);
    }
  }

  // Try to attach an already used phone number
  const duplicateResult = await waitlist.attachPhone('user0', '+15550000001');
  if (duplicateResult !== false) {
    throw new Error('Should not be able to attach already used phone number');
  }

  const originalMapping = await waitlist._getPhoneMapping('+15550000001');
  if (originalMapping !== 'user1') {
    throw new Error(`Original phone mapping was affected. Expected user1, got ${originalMapping}`);
  }
}

async function testEmailAttachment(): Promise<void> {
  // Insert 20 users with phone numbers only
  for (let i = 0; i < 20; i++) {
    await waitlist.insertUser(`user${i}`, {
      phone: `+1555000${i.toString().padStart(4, '0')}`,
      metadata: { name: `User ${i}` }
    });
  }

  // Attach emails to all users
  const attachResults = await Promise.all(
    Array.from({ length: 20 }, (_, i) => 
      waitlist.attachEmail(`user${i}`, `user${i}@test.com`)
    )
  );

  if (!attachResults.every(result => result === true)) {
    throw new Error('Not all email attachments succeeded');
  }

  // Verify email mappings
  for (let i = 0; i < 20; i++) {
    const userId = `user${i}`;
    const email = `user${i}@test.com`;
    const mappedId = await waitlist._getEmailMapping(email);
    
    if (mappedId !== userId) {
      throw new Error(`Email mapping incorrect for ${userId}. Expected ${userId}, got ${mappedId}`);
    }
  }

  // Try to attach an already used email
  const duplicateResult = await waitlist.attachEmail('user0', 'user1@test.com');
  if (duplicateResult !== false) {
    throw new Error('Should not be able to attach already used email');
  }

  const originalMapping = await waitlist._getEmailMapping('user1@test.com');
  if (originalMapping !== 'user1') {
    throw new Error(`Original email mapping was affected. Expected user1, got ${originalMapping}`);
  }
}

async function testLargeListBumpToTop(): Promise<void> {
  // Insert first 5K users in parallel
  let insertOperations = Array.from({ length: 5000 }, (_, i) => {
    return waitlist.insertUser(`user${i}`, {
      email: `user${i}@test.com`,
      metadata: { name: `User ${i}` }
    });
  });
  await Promise.all(insertOperations);

  // Insert second 5K users in parallel
  insertOperations = Array.from({ length: 5000 }, (_, i) => {
    const userNum = i + 5000;
    return waitlist.insertUser(`user${userNum}`, {
      email: `user${userNum}@test.com`,
      metadata: { name: `User ${userNum}` }
    });
  });
  await Promise.all(insertOperations);

  // Move last user to top
  const lastUserId = `user${9999}`;
  await waitlist.bumpUserUp(lastUserId, 1);

  // Check that last user is now first
  const topPosition = await waitlist.getPosition(lastUserId);
  if (topPosition !== 1) {
    throw new Error(`Expected ${lastUserId} at position 1, found at ${topPosition}`);
  }

  // Get final order and verify it's correct
  const finalOrder = await waitlist._getOrderedIds();
  
  if (finalOrder.length !== 10000) {
    throw new Error(`Expected 10000 users, found ${finalOrder.length}`);
  }

  // Verify order: last user should be first, rest should be in original order
  const expectedOrder = [lastUserId].concat(
    Array.from({ length: 9999 }, (_, i) => `user${i}`)
  );

  // First verify all positions in order
  for (let i = 0; i < 10000; i++) {
    if (finalOrder[i] !== expectedOrder[i]) {
      throw new Error(
        `Position ${i + 1} incorrect.\n` +
        `Expected: ${expectedOrder[i]}\n` +
        `Got: ${finalOrder[i]}`
      );
    }
  }

  // Then verify email mappings in parallel batches
  const BATCH_SIZE = 200;
  
  for (let startIdx = 0; startIdx < 10000; startIdx += BATCH_SIZE) {
    const endIdx = Math.min(startIdx + BATCH_SIZE, 10000);
    
    const verificationPromises = Array.from({ length: endIdx - startIdx }, (_, idx) => {
      const i = startIdx + idx;
      const userId = finalOrder[i];
      const email = `${userId}@test.com`;
      return waitlist._getEmailMapping(email).then(mappedId => {
        if (mappedId !== userId) {
          throw new Error(`Email mapping incorrect for ${userId}. Expected ${userId}, got ${mappedId}`);
        }
      });
    });

    await Promise.all(verificationPromises);
  }
}

async function testDeleteAndReinsert(): Promise<void> {
  // Insert 20 users sequentially
  for (let i = 0; i < 20; i++) {
    await waitlist.insertUser(`user${i}`, {
      email: `user${i}@test.com`,
      phone: `+1555${i.toString().padStart(4, '0')}`,
      metadata: { name: `User ${i}` }
    });
  }

  // Delete all users one by one
  for (let i = 0; i < 20; i++) {
    const success = await waitlist.deleteUser(`user${i}`);
    if (!success) {
      throw new Error(`Failed to delete user${i}`);
    }
  }

  // Verify list is empty
  const remainingIds = await waitlist._getOrderedIds();
  if (remainingIds.length !== 0) {
    throw new Error(`Expected empty list, but found ${remainingIds.length} users`);
  }

  // Verify email and phone mappings are cleared
  for (let i = 0; i < 20; i++) {
    const email = `user${i}@test.com`;
    const phone = `+1555${i.toString().padStart(4, '0')}`;
    
    const emailMapping = await waitlist._getEmailMapping(email);
    const phoneMapping = await waitlist._getPhoneMapping(phone);
    
    if (emailMapping !== null) {
      throw new Error(`Email mapping still exists for ${email}`);
    }
    if (phoneMapping !== null) {
      throw new Error(`Phone mapping still exists for ${phone}`);
    }
  }

  // Re-insert 20 new users
  for (let i = 0; i < 20; i++) {
    const result = await waitlist.insertUser(`new_user${i}`, {
      email: `new_user${i}@test.com`,
      phone: `+1666${i.toString().padStart(4, '0')}`,
      metadata: { name: `New User ${i}` }
    });

    // Verify position is correct (should be i + 1 since we're inserting sequentially)
    if (result.position !== i + 1) {
      throw new Error(`Expected new_user${i} at position ${i + 1}, got ${result.position}`);
    }
  }

  // Final verification of all positions
  for (let i = 0; i < 20; i++) {
    const position = await waitlist.getPosition(`new_user${i}`);
    if (position !== i + 1) {
      throw new Error(`Expected new_user${i} at position ${i + 1}, got ${position}`);
    }
  }
}

async function testDeleteByEmailAndPhone(): Promise<void> {
  // Insert 20 users sequentially
  for (let i = 0; i < 20; i++) {
    await waitlist.insertUser(`user${i}`, {
      email: `user${i}@test.com`,
      phone: `+1555${i.toString().padStart(4, '0')}`,
      metadata: { name: `User ${i}` }
    });
  }

  // Delete user5 by email and user6 by phone
  const deleteEmailResult = await waitlist.deleteUserByEmail('user5@test.com');
  if (!deleteEmailResult) {
    throw new Error('Failed to delete user5 by email');
  }

  const deletePhoneResult = await waitlist.deleteUserByPhone('+15550006');
  if (!deletePhoneResult) {
    throw new Error('Failed to delete user6 by phone');
  }

  // Get final list and verify count
  const remainingIds = await waitlist._getOrderedIds();
  if (remainingIds.length !== 18) {
    throw new Error(`Expected 18 users, but found ${remainingIds.length}`);
  }

  // Verify positions of all remaining users
  for (let i = 0; i < 20; i++) {
    // Skip deleted users
    if (i === 5 || i === 6) continue;

    const position = await waitlist.getPosition(`user${i}`);
    const expectedPosition = i < 5 ? i + 1 : i - 1;
    
    if (position !== expectedPosition) {
      throw new Error(`Wrong position for user${i}: expected ${expectedPosition}, got ${position}`);
    }
  }

  // Verify deleted users' email and phone mappings are removed
  const email5Mapping = await waitlist._getEmailMapping('user5@test.com');
  const phone5Mapping = await waitlist._getPhoneMapping('+15550005');
  const email6Mapping = await waitlist._getEmailMapping('user6@test.com');
  const phone6Mapping = await waitlist._getPhoneMapping('+15550006');

  if (email5Mapping !== null || phone5Mapping !== null) {
    throw new Error('User5 mappings still exist');
  }
  if (email6Mapping !== null || phone6Mapping !== null) {
    throw new Error('User6 mappings still exist');
  }

  // Verify remaining users' mappings are intact
  for (let i = 0; i < 20; i++) {
    if (i === 5 || i === 6) continue;

    const email = `user${i}@test.com`;
    const phone = `+1555${i.toString().padStart(4, '0')}`;
    
    const emailMapping = await waitlist._getEmailMapping(email);
    const phoneMapping = await waitlist._getPhoneMapping(phone);
    
    if (emailMapping !== `user${i}`) {
      throw new Error(`Email mapping incorrect for user${i}`);
    }
    if (phoneMapping !== `user${i}`) {
      throw new Error(`Phone mapping incorrect for user${i}`);
    }
  }
}

async function testDeleteAndBumpUp(): Promise<void> {
  // Insert 20 users sequentially
  for (let i = 0; i < 20; i++) {
    await waitlist.insertUser(`user${i}`, {
      email: `user${i}@test.com`,
      phone: `+1555${i.toString().padStart(4, '0')}`,
      metadata: { name: `User ${i}` }
    });
  }

  // Delete user5 by email and user6 by phone
  const deleteEmailResult = await waitlist.deleteUserByEmail('user5@test.com');
  if (!deleteEmailResult) {
    throw new Error('Failed to delete user5 by email');
  }

  const deletePhoneResult = await waitlist.deleteUserByPhone('+15550006');
  if (!deletePhoneResult) {
    throw new Error('Failed to delete user6 by phone');
  }

  // Move last user (user19) to position 3
  await waitlist.bumpUserUp('user19', 3);

  // Get final order and verify count
  const finalOrder = await waitlist._getOrderedIds();
  if (finalOrder.length !== 18) {
    throw new Error(`Expected 18 users, but found ${finalOrder.length}`);
  }

  // Expected order: user0, user1, user19, user2, user3, user4, user7, user8, ..., user18
  const expectedOrder = [
    'user0', 'user1', 'user19', 'user2', 'user3', 'user4',
    ...Array.from({ length: 12 }, (_, i) => `user${i + 7}`)
  ];

  if (JSON.stringify(finalOrder) !== JSON.stringify(expectedOrder)) {
    throw new Error(
      'Incorrect final order.\n' +
      `Expected: ${expectedOrder.join(', ')}\n` +
      `Got: ${finalOrder.join(', ')}`
    );
  }

  // Verify positions of all remaining users
  for (const [index, userId] of finalOrder.entries()) {
    const position = await waitlist.getPosition(userId);
    const expectedPosition = index + 1;
    
    if (position !== expectedPosition) {
      throw new Error(`Wrong position for ${userId}: expected ${expectedPosition}, got ${position}`);
    }
  }

  // Verify email and phone mappings are correct
  for (const userId of finalOrder) {
    const email = `${userId}@test.com`;
    const phone = `+1555${userId.replace('user', '').padStart(4, '0')}`;
    
    const emailMapping = await waitlist._getEmailMapping(email);
    const phoneMapping = await waitlist._getPhoneMapping(phone);
    
    if (emailMapping !== userId) {
      throw new Error(`Email mapping incorrect for ${userId}`);
    }
    if (phoneMapping !== userId) {
      throw new Error(`Phone mapping incorrect for ${userId}`);
    }
  }

  // Verify deleted users' mappings are removed
  const deletedMappings = await Promise.all([
    waitlist._getEmailMapping('user5@test.com'),
    waitlist._getPhoneMapping('+15550005'),
    waitlist._getEmailMapping('user6@test.com'),
    waitlist._getPhoneMapping('+15550006')
  ]);

  if (deletedMappings.some(mapping => mapping !== null)) {
    throw new Error('Some mappings for deleted users still exist');
  }
}

// Main test runner
async function runTests() {
  let hasError = false;
  
  try {
    const tests = [
      ['Concurrent Email Users', testConcurrentEmailUsers],
      ['Concurrent Duplicate User Email', testConcurrentDuplicateUserEmail],
      ['Ordered Bump Up', testOrderedBumpUp],
      ['Large Scale Bump Up', testLargeScaleBumpUp],
      ['Repeated Bump Up', testRepeatedBumpUp],
      ['Step by Step Bump Up', testStepByStepBumpUp],
      ['Concurrent Bump to Same Position', testConcurrentBumpToSamePosition],
      ['Phone Attachment', testPhoneAttachment],
      ['Email Attachment', testEmailAttachment],
      ['Large List Bump to Top', testLargeListBumpToTop],
      ['Delete and Reinsert', testDeleteAndReinsert],
      ['Delete by Email and Phone', testDeleteByEmailAndPhone],
      ['Delete and Bump Up', testDeleteAndBumpUp]
    ] as const;

    for (const [name, testFn] of tests) {
      try {
        // Create fresh connection for each test
        waitlist = new WaitlistManager({
          host: process.env.REDIS_HOST || 'localhost',
          port: parseInt(process.env.REDIS_PORT || '6379'),
          password: process.env.REDIS_PASSWORD || '',
        });
        
        await clearRedis();
        await testFn();
        console.log(`${GREEN_CHECK} ${name}`);
        
        // Cleanup after test
        await forceCleanup();
      } catch (err) {
        hasError = true;
        console.log(`${RED_X} ${name}`);
        console.error('\x1b[31m', (err as Error).message || 'Unknown error', '\x1b[0m');
        break; // Stop on first error
      }
    }
  } finally {
    process.exit(hasError ? 1 : 0);
  }
}

// Run the tests
runTests();