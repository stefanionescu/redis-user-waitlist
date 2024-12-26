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

async function testConcurrentDuplicateUserPhone() {
  // Create initial user with phone
  const phone = '+15551234567';
  await waitlist.insertUser('user1', {
    phone,
    metadata: { name: 'Original User' }
  });

  // Try to insert multiple users with the same phone concurrently
  const attempts = 5;
  const operations = [];
  
  for (let i = 0; i < attempts; i++) {
    operations.push(waitlist.insertUser(`user${i + 2}`, {
      phone,
      metadata: { name: `Duplicate User ${i + 1}` }
    }));
  }

  // Run all operations concurrently
  const results = await Promise.all(operations.map(p => p.catch(e => e)));

  // Verify all attempts either returned the same position or failed
  const position = await waitlist.getPosition('user1');
  let successCount = 0;

  for (const result of results) {
    if (result instanceof Error) {
      continue;
    }
    successCount++;
    if (result.position !== position) {
      throw new Error(`Expected position ${position}, but got ${result.position}`);
    }
    if (!result.already_existed) {
      throw new Error('Expected already_existed to be true');
    }
  }

  // Verify only one user with this phone exists
  const finalCount = await waitlist.getLength();
  if (finalCount !== 1) {
    throw new Error(`Expected 1 user, but found ${finalCount}`);
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
  await waitlist.moveUser('user8', 4);
  
  // Get actual order
  const actualOrder = await waitlist.getOrderedIds();

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
    const mappedId = await waitlist.getEmailMapping(email);
    if (mappedId !== userId) {
      throw new Error(`Email mapping incorrect for ${userId}. Expected ${userId}, got ${mappedId}`);
    }
  }
}

async function testLargeScaleBumpUp(): Promise<void> {
  // First verify Redis is clean
  const initialCount = await waitlist.getOrderedIds();
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
  const afterInsertCount = await waitlist.getOrderedIds();
  if (afterInsertCount.length !== 2000) {
    throw new Error(`Wrong number of users after insert. Expected 2000, got ${afterInsertCount.length}`);
  }

  // Move users 500-1000 to positions 400-900
  for (let i = 499; i < 1000; i++) {
    await waitlist.moveUser(`mass_user${i}`, i - 99);
  }

  // Get final count and order
  const finalOrder = await waitlist.getOrderedIds();
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
    await waitlist.moveUser(`bump_user${userToMove}`, targetPos);
  }

  // Get final order
  const finalOrder = await waitlist.getOrderedIds();

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
    const mappedId = await waitlist.getEmailMapping(email);
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
    await waitlist.moveUser('bump_user19', targetPos);
    const currentOrder = await waitlist.getOrderedIds();

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
  const finalOrder = await waitlist.getOrderedIds();

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
    const mappedId = await waitlist.getEmailMapping(email);
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
    waitlist.moveUser('bump_user16', 2),
    waitlist.moveUser('bump_user17', 2),
    waitlist.moveUser('bump_user18', 2),
    waitlist.moveUser('bump_user19', 2)
  ]);

  // Get final order
  const finalOrder = await waitlist.getOrderedIds();

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
    const mappedId = await waitlist.getEmailMapping(email);
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
    const mappedId = await waitlist.getPhoneMapping(phone);
    
    if (mappedId !== userId) {
      throw new Error(`Phone mapping incorrect for ${userId}. Expected ${userId}, got ${mappedId}`);
    }
  }

  // Try to attach an already used phone number
  const duplicateResult = await waitlist.attachPhone('user0', '+15550000001');
  if (duplicateResult !== false) {
    throw new Error('Should not be able to attach already used phone number');
  }

  const originalMapping = await waitlist.getPhoneMapping('+15550000001');
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
    const mappedId = await waitlist.getEmailMapping(email);
    
    if (mappedId !== userId) {
      throw new Error(`Email mapping incorrect for ${userId}. Expected ${userId}, got ${mappedId}`);
    }
  }

  // Try to attach an already used email
  const duplicateResult = await waitlist.attachEmail('user0', 'user1@test.com');
  if (duplicateResult !== false) {
    throw new Error('Should not be able to attach already used email');
  }

  const originalMapping = await waitlist.getEmailMapping('user1@test.com');
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
  await waitlist.moveUser(lastUserId, 1);

  // Check that last user is now first
  const topPosition = await waitlist.getPosition(lastUserId);
  if (topPosition !== 1) {
    throw new Error(`Expected ${lastUserId} at position 1, found at ${topPosition}`);
  }

  // Get final order and verify it's correct
  const finalOrder = await waitlist.getOrderedIds();
  
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
      return waitlist.getEmailMapping(email).then(mappedId => {
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
  const remainingIds = await waitlist.getOrderedIds();
  if (remainingIds.length !== 0) {
    throw new Error(`Expected empty list, but found ${remainingIds.length} users`);
  }

  // Verify email and phone mappings are cleared
  for (let i = 0; i < 20; i++) {
    const email = `user${i}@test.com`;
    const phone = `+1555${i.toString().padStart(4, '0')}`;
    
    const emailMapping = await waitlist.getEmailMapping(email);
    const phoneMapping = await waitlist.getPhoneMapping(phone);
    
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
  const remainingIds = await waitlist.getOrderedIds();
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
  const email5Mapping = await waitlist.getEmailMapping('user5@test.com');
  const phone5Mapping = await waitlist.getPhoneMapping('+15550005');
  const email6Mapping = await waitlist.getEmailMapping('user6@test.com');
  const phone6Mapping = await waitlist.getPhoneMapping('+15550006');

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
    
    const emailMapping = await waitlist.getEmailMapping(email);
    const phoneMapping = await waitlist.getPhoneMapping(phone);
    
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
  await waitlist.moveUser('user19', 3);

  // Get final order and verify count
  const finalOrder = await waitlist.getOrderedIds();
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
    
    const emailMapping = await waitlist.getEmailMapping(email);
    const phoneMapping = await waitlist.getPhoneMapping(phone);
    
    if (emailMapping !== userId) {
      throw new Error(`Email mapping incorrect for ${userId}`);
    }
    if (phoneMapping !== userId) {
      throw new Error(`Phone mapping incorrect for ${userId}`);
    }
  }

  // Verify deleted users' mappings are removed
  const deletedMappings = await Promise.all([
    waitlist.getEmailMapping('user5@test.com'),
    waitlist.getPhoneMapping('+15550005'),
    waitlist.getEmailMapping('user6@test.com'),
    waitlist.getPhoneMapping('+15550006')
  ]);

  if (deletedMappings.some(mapping => mapping !== null)) {
    throw new Error('Some mappings for deleted users still exist');
  }
}

async function testLimitAndConcurrentOperations(): Promise<void> {
  try {
    // Set a small limit
    waitlist.setLengthLimit(5);

    // Try to insert 10 users concurrently
    const operations = Array.from({ length: 10 }, async (_, i) => {
      return waitlist.insertUser(`user${i}`, {
        email: `user${i}@test.com`,
        metadata: { name: `User ${i}` }
      }).catch(e => e);  // Catch errors for failed inserts
    });

    const results = await Promise.all(operations);
    
    // Count successful inserts
    const successfulInserts = results.filter(r => !(r instanceof Error));
    if (successfulInserts.length !== 5) {
      throw new Error(`Expected 5 successful inserts, got ${successfulInserts.length}`);
    }

    // Verify failed inserts were due to limit
    const failedInserts = results.filter(r => r instanceof Error);
    if (!failedInserts.every(e => e.message.includes('Waitlist is full'))) {
      throw new Error('Some operations failed for unexpected reasons');
    }
  } finally {
    // Reset limit to default value
    waitlist.setLengthLimit(100000);
  }
}

async function testLargeScaleInsertAndLength(): Promise<void> {
  const TOTAL_USERS = 50000;
  const BATCH_SIZE = 5000;
  const NUM_BATCHES = TOTAL_USERS / BATCH_SIZE;

  // Insert users in batches
  for (let batch = 0; batch < NUM_BATCHES; batch++) {
    const batchStart = batch * BATCH_SIZE;
    
    const batchOperations = Array.from({ length: BATCH_SIZE }, async (_, i) => {
      const userIndex = batchStart + i;
      return waitlist.insertUser(`user${userIndex}`, {
        email: `mass_user${userIndex}@test.com`,
        metadata: { name: `Mass User ${userIndex}` }
      });
    });

    await Promise.all(batchOperations);
  }

  // Verify final length
  const finalLength = await waitlist.getLength();
  if (finalLength !== TOTAL_USERS) {
    throw new Error(`Expected length ${TOTAL_USERS}, but got ${finalLength}`);
  }
}

async function testConcurrentDeleteAndBump(): Promise<void> {
  // Insert 21 users sequentially (0-20)
  for (let i = 0; i <= 20; i++) {
    await waitlist.insertUser(`user${i}`, {
      email: `user${i}@test.com`,
      phone: `+1555${i.toString().padStart(4, '0')}`,
      metadata: { name: `User ${i}` }
    });
  }

  // Test concurrent operations: delete 4-7 and bump 17-20 to those positions
  const operations = [
    // Delete operations
    waitlist.deleteUser('user4'),
    waitlist.deleteUser('user5'),
    waitlist.deleteUser('user6'),
    waitlist.deleteUser('user7'),
    // Bump operations
    waitlist.moveUser('user17', 4),
    waitlist.moveUser('user18', 5),
    waitlist.moveUser('user19', 6),
    waitlist.moveUser('user20', 7)
  ];

  // Run operations concurrently
  await Promise.all(operations);

  // Verify deleted users are gone
  for (let i = 4; i <= 7; i++) {
    const position = await waitlist.getPosition(`user${i}`);
    if (position !== 0) {
      throw new Error(`User${i} should be deleted but found at position ${position}`);
    }
  }

  // Verify bumped users are in correct positions
  const expectedPositions = {
    'user17': 4,
    'user18': 5,
    'user19': 6,
    'user20': 7
  };

  for (const [userId, expectedPos] of Object.entries(expectedPositions)) {
    const position = await waitlist.getPosition(userId);
    if (position !== expectedPos) {
      throw new Error(`${userId} should be at position ${expectedPos}, but found at ${position}`);
    }
  }

  // Verify remaining users maintained their relative order
  const finalOrder = await waitlist.getOrderedIds();
  const expectedOrder = [
    'user0', 'user1', 'user2',
    'user17', 'user18', 'user19', 'user20',
    'user3',  // user3 gets pushed back after the bumped users
    'user8', 'user9', 'user10', 'user11', 'user12',
    'user13', 'user14', 'user15', 'user16'
  ];

  if (JSON.stringify(finalOrder) !== JSON.stringify(expectedOrder)) {
    throw new Error(
      'Incorrect final order.\n' +
      `Expected: ${expectedOrder.join(', ')}\n` +
      `Got: ${finalOrder.join(', ')}`
    );
  }

  // Verify final count
  if (finalOrder.length !== 17) { // 21 initial - 4 deleted
    throw new Error(`Expected 17 users, but found ${finalOrder.length}`);
  }
}

async function testBasicInviteCodeFlow(): Promise<void> {
  // Create initial user
  await waitlist.insertUser('creator1', {
    email: 'creator1@test.com',
    metadata: { name: 'Creator 1' }
  });

  // Create invite code
  const code = await waitlist.createInviteCode('creator1', 3);
  
  // Verify code exists and creator
  const creator = await waitlist.getInviteCodeCreator(code);
  if (!creator) {
    throw new Error('Invite code was not stored properly');
  }
  if (creator !== 'creator1') {
    throw new Error(`Wrong creator for code. Expected creator1, got ${creator}`);
  }

  // Verify bump positions
  const bumpPos = await waitlist.getInviteCodeBumpPositions(code);
  if (bumpPos !== 3) {
    throw new Error(`Wrong bump positions. Expected 3, got ${bumpPos}`);
  }
  
  // Use invite code
  const result = await waitlist.useInviteCode(code, 'invited1', {
    email: 'invited1@test.com',
    metadata: { name: 'Invited User 1' }
  }, 0);

  // Verify positions
  const creatorPosition = await waitlist.getPosition('creator1');
  if (creatorPosition !== 1) {
    throw new Error(`Expected creator at position 1, got ${creatorPosition}`);
  }

  if (result.position !== 2) {
    throw new Error(`Expected invited user at position 2, got ${result.position}`);
  }
}

async function testLargeScaleInviteCodes(): Promise<void> {
  const CREATORS = 100;
  const CODES_PER_CREATOR = 3;
  const TOTAL_USERS = CREATORS * 2;  // Creators + one invited user per creator

  // Create creators (100 users)
  const creatorOperations = Array.from({ length: CREATORS }, (_, i) => 
    waitlist.insertUser(`creator${i}`, {
      email: `creator${i}@test.com`,
      metadata: { name: `Creator ${i}` }
    })
  );
  await Promise.all(creatorOperations);

  // Verify creators were added
  const creatorCount = await waitlist.getLength();
  if (creatorCount !== CREATORS) {
    throw new Error(`Expected ${CREATORS} creators, got ${creatorCount}`);
  }

  // Generate codes (300 codes total)
  const codes: { creatorId: string, code: string }[] = [];
  for (let i = 0; i < CREATORS; i++) {
    for (let j = 0; j < CODES_PER_CREATOR; j++) {
      const code = await waitlist.createInviteCode(`creator${i}`, j + 1);
      codes.push({ creatorId: `creator${i}`, code });
    }
  }

  // Use one code per creator (100 uses total)
  const useOperations = codes.slice(0, CREATORS).map((codeInfo, i) =>
    waitlist.useInviteCode(
      codeInfo.code,
      `invited${i}`,
      {
        email: `invited${i}@test.com`,
        metadata: { name: `Invited User ${i}` }
      },
      i % 3
    ).catch(error => {
      console.error(`Failed to use code ${codeInfo.code}:`, error.message);
      throw error;
    })
  );

  const results = await Promise.allSettled(useOperations);
  const failures = results.filter(r => r.status === 'rejected');
  
  if (failures.length > 0) {
    console.error(`${failures.length} invite code uses failed:`);
    failures.forEach(f => console.error((f as PromiseRejectedResult).reason));
    throw new Error(`Failed to use ${failures.length} invite codes`);
  }

  // Verify final state
  const finalLength = await waitlist.getLength();
  
  if (finalLength !== TOTAL_USERS) {
    const allUsers = await waitlist.getOrderedIds();
    console.log('All users:', allUsers);
    throw new Error(`Expected ${TOTAL_USERS} users, got ${finalLength}`);
  }

  // Verify all users exist
  for (let i = 0; i < CREATORS; i++) {
    const creatorExists = await waitlist.getPosition(`creator${i}`);
    const invitedExists = await waitlist.getPosition(`invited${i}`);
    
    if (!creatorExists) {
      throw new Error(`Creator ${i} not found`);
    }
    if (!invitedExists) {
      throw new Error(`Invited user ${i} not found`);
    }
  }
}

async function testInviteCodeLimits(): Promise<void> {
  // Create user
  await waitlist.insertUser('limitTester', {
    email: 'limit@test.com',
    metadata: { name: 'Limit Tester' }
  });

  // Set low invite code limit
  waitlist.setInviteCodeLimit(2);

  // Try to create more codes than allowed
  const code1 = await waitlist.createInviteCode('limitTester', 1);
  const code2 = await waitlist.createInviteCode('limitTester', 1);
  
  try {
    await waitlist.createInviteCode('limitTester', 1);
    throw new Error('Should not allow creating more than limit');
  } catch (e) {
    if (!(e instanceof Error) || !e.message.includes('Invite code limit reached')) {
      throw e;
    }
  }

  // Verify existing codes still work
  await waitlist.useInviteCode(code1, 'invited1', {
    email: 'invited1@test.com',
    metadata: { name: 'Invited 1' }
  }, 1);

  await waitlist.useInviteCode(code2, 'invited2', {
    email: 'invited2@test.com',
    metadata: { name: 'Invited 2' }
  }, 1);

  // Reset limit
  waitlist.setInviteCodeLimit(3);
}

async function testConcurrentInviteCodeUse(): Promise<void> {
  // Create initial user
  await waitlist.insertUser('multiCreator', {
    email: 'multi@test.com',
    metadata: { name: 'Multi Creator' }
  });

  // Create a single invite code
  const code = await waitlist.createInviteCode('multiCreator', 2);

  // Try to use same code concurrently
  const attempts = 5;
  const operations = Array.from({ length: attempts }, (_, i) =>
    waitlist.useInviteCode(
      code,
      `concurrent${i}`,
      {
        email: `concurrent${i}@test.com`,
        metadata: { name: `Concurrent User ${i}` }
      },
      2
    ).catch(e => e)
  );

  const results = await Promise.all(operations);
  
  // Verify only one succeeded
  const successes = results.filter(r => !(r instanceof Error));
  if (successes.length !== 1) {
    throw new Error(`Expected 1 successful use, got ${successes.length}`);
  }

  // Verify others failed appropriately
  const failures = results.filter(r => r instanceof Error);
  if (!failures.every(e => e.message.includes('Invite code already used'))) {
    throw new Error('Unexpected error message in failures');
  }
}

async function testInviteCodePositionEdgeCases(): Promise<void> {
  // Insert 20 users sequentially
  for (let i = 0; i < 20; i++) {
    await waitlist.insertUser(`user${i}`, {
      email: `user${i}@test.com`,
      phone: `+1555${i.toString().padStart(4, '0')}`,
      metadata: { name: `User ${i}` }
    });
  }

  // Create invite code from user15 with large bump (100)
  const largeCode = await waitlist.createInviteCode('user15', 100);
  
  // Use the code and verify actual position
  await waitlist.useInviteCode(largeCode, 'invited1', {
    email: 'invited1@test.com',
    metadata: { name: 'Invited User 1' }
  }, 0);

  // Verify user15 moved to position 1
  const user15NewPos = await waitlist.getPosition('user15');
  if (user15NewPos !== 1) {
    throw new Error(`Expected user15 at position 1, got ${user15NewPos}`);
  }

  // Get user10's new position after user15's move
  const user10PosAfterFirst = await waitlist.getPosition('user10');

  // Create another code with small bump (2) and test position prediction
  const smallCode = await waitlist.createInviteCode('user10', 2);
  const smallPredictedPos = await waitlist.getPositionAfterInviteCodeUse(smallCode);
  
  // Calculate expected position based on current position and bump amount
  const expectedPos = Math.max(1, user10PosAfterFirst - 2);
  
  if (smallPredictedPos !== expectedPos) {
    throw new Error(`Expected predicted position ${expectedPos}, got ${smallPredictedPos}`);
  }

  // Use the small bump code
  await waitlist.useInviteCode(smallCode, 'invited2', {
    email: 'invited2@test.com',
    metadata: { name: 'Invited User 2' }
  }, 0);

  // Verify final position
  const finalUser10Pos = await waitlist.getPosition('user10');
  if (finalUser10Pos !== expectedPos) {
    throw new Error(`Expected user10 at position ${expectedPos}, got ${finalUser10Pos}`);
  }

  // Verify final order
  const finalOrder = await waitlist.getOrderedIds();
  if (finalOrder[0] !== 'user15' || finalOrder[expectedPos - 1] !== 'user10') {
    throw new Error('Final order is incorrect');
  }
}

async function testComplexConcurrentOperations(): Promise<void> {
  // Setup initial users (20 users)
  for (let i = 0; i < 20; i++) {
    await waitlist.insertUser(`base${i}`, {
      email: `base${i}@test.com`,
      metadata: { name: `Base User ${i}` }
    });
  }

  // Create invite codes with different bump positions
  const inviteCodes = [];
  for (let i = 0; i < 5; i++) {
    const code = await waitlist.createInviteCode(`base${i * 2}`, i + 1);
    inviteCodes.push({ code, creator: `base${i * 2}`, bumpPositions: i + 1 });
  }

  // Prepare concurrent operations
  const operations = [
    // 5 deletes (every 3rd user starting from base1)
    ...Array.from({ length: 5 }, (_, i) => 
      waitlist.deleteUser(`base${1 + i * 3}`)),

    // 10 new inserts
    ...Array.from({ length: 10 }, (_, i) =>
      waitlist.insertUser(`new${i}`, {
        email: `new${i}@test.com`,
        metadata: { name: `New User ${i}` }
      })),

    // 5 invite code uses
    ...inviteCodes.map((invite, i) =>
      waitlist.useInviteCode(
        invite.code,
        `invited${i}`,
        {
          email: `invited${i}@test.com`,
          metadata: { name: `Invited User ${i}` }
        },
        invite.bumpPositions
      ))
  ];

  // Run all operations concurrently
  const results = await Promise.allSettled(operations);

  // Check for failures
  const failures = results.filter(r => r.status === 'rejected');
  if (failures.length > 0) {
    console.error('Failed operations:', failures);
    throw new Error(`${failures.length} operations failed`);
  }
  
  // Verify all expected users exist
  const shouldExist = [
    // Original users that weren't deleted (filter out nulls before creating array)
    ...Array.from({ length: 20 }, (_, i) => 
      (i - 1) % 3 === 0 ? undefined : `base${i}`).filter((id): id is string => id !== undefined),
    // New users
    ...Array.from({ length: 10 }, (_, i) => `new${i}`),
    // Invited users
    ...Array.from({ length: 5 }, (_, i) => `invited${i}`)
  ];

  for (const id of shouldExist) {
    const exists = await waitlist.getPosition(id);
    if (!exists) {
      throw new Error(`Expected user ${id} not found`);
    }
  }

  // Verify deleted users don't exist
  const shouldNotExist = Array.from({ length: 5 }, (_, i) => `base${1 + i * 3}`);
  for (const id of shouldNotExist) {
    const exists = await waitlist.getPosition(id);
    if (exists) {
      throw new Error(`Deleted user ${id} still exists at position ${exists}`);
    }
  }

  // Verify invite code creators are in correct positions relative to their invitees
  for (const { creator, bumpPositions } of inviteCodes) {
    const creatorPos = await waitlist.getPosition(creator);
    if (creatorPos === 0) continue; // Skip if creator was deleted
    
    // Find invited user's position
    const invitedUser = `invited${inviteCodes.findIndex(ic => ic.creator === creator)}`;
    const invitedPos = await waitlist.getPosition(invitedUser);
    
    // Verify creator is ahead of their invited user
    if (creatorPos >= invitedPos) {
      throw new Error(
        `Creator ${creator} at position ${creatorPos} should be ahead of invited user ${invitedUser} at position ${invitedPos}`
      );
    }
  }

  // Verify total length
  const expectedLength = await (async () => {
    // Start with 20 base users
    const baseUsers = 20;
    
    // Subtract 5 deleted users (every 3rd user starting from base1)
    const deletedUsers = 5;  // base1, base4, base7, base10, base13
    
    // Add 10 new users
    const newUsers = 10;  // new0 through new9
    
    // Add 5 invited users
    const invitedUsers = 5;  // invited0 through invited4
    
    return baseUsers - deletedUsers + newUsers + invitedUsers;  // 20 - 5 + 10 + 5 = 30
  })();

  const actualLength = await waitlist.getLength();
  if (actualLength !== expectedLength) {
    throw new Error(`Expected ${expectedLength} users, got ${actualLength}`);
  }
}

async function testLargeScaleBackwardBump(): Promise<void> {
  const TOTAL_USERS = 2000;
  const BUMP_START = 400;
  const BUMP_END = 1000;
  const BUMP_POSITIONS = 99;

  // Insert users sequentially
  const insertOperations = Array.from({ length: TOTAL_USERS }, (_, i) =>
    waitlist.insertUser(`user${i}`, {
      email: `user${i}@test.com`,
      metadata: { name: `User ${i}` }
    })
  );
  await Promise.all(insertOperations);

  // Move users backward one at a time (to higher numbers)
  for (let i = BUMP_END; i >= BUMP_START; i--) {  // Move in REVERSE order
    const userId = `user${i}`;
    const targetPos = Math.min(TOTAL_USERS, i + 1 + BUMP_POSITIONS);
    await waitlist.moveUser(userId, targetPos);
  }

  // Verify final positions
  for (let i = BUMP_START; i <= BUMP_END; i++) {
    const userId = `user${i}`;
    const pos = await waitlist.getPosition(userId);
    const expectedPos = Math.min(TOTAL_USERS, i + 1 + BUMP_POSITIONS);
    
    if (pos !== expectedPos) {
      throw new Error(`User ${userId} at wrong position. Expected ${expectedPos}, got ${pos}`);
    }
  }

  // Verify total length hasn't changed
  const finalLength = await waitlist.getLength();
  if (finalLength !== TOTAL_USERS) {
    throw new Error(`Expected ${TOTAL_USERS} users, got ${finalLength}`);
  }
}

async function testRepeatedLargeScaleMovement(): Promise<void> {
  const TOTAL_USERS = 10000;
  const BATCH_SIZE = 5;  // 2000 batches of 5 users each
  const TARGET_USER = 'user5000';
  const MOVEMENT_CYCLES = 100;

  // Insert users in batches
  for (let batchStart = 0; batchStart < TOTAL_USERS; batchStart += BATCH_SIZE) {
    const batchOperations = Array.from({ length: BATCH_SIZE }, (_, i) => {
      const userId = `user${batchStart + i}`;
      return waitlist.insertUser(userId, {
        email: `${userId}@test.com`,
        metadata: { name: `User ${batchStart + i}` }
      });
    });
    await Promise.all(batchOperations);
  }

  // Record initial position of target user
  const initialPosition = await waitlist.getPosition(TARGET_USER);
  if (initialPosition !== 5001) { // 0-based to 1-based indexing
    throw new Error(`Initial position wrong. Expected 5001, got ${initialPosition}`);
  }

  // Move user back and forth repeatedly
  for (let cycle = 0; cycle < MOVEMENT_CYCLES; cycle++) {
    // Move to position 1
    await waitlist.moveUser(TARGET_USER, 1);
    const frontPos = await waitlist.getPosition(TARGET_USER);
    if (frontPos !== 1) {
      throw new Error(`Cycle ${cycle}: Front position wrong. Expected 1, got ${frontPos}`);
    }

    // Move back to original position
    await waitlist.moveUser(TARGET_USER, initialPosition);
    const backPos = await waitlist.getPosition(TARGET_USER);
    if (backPos !== initialPosition) {
      throw new Error(`Cycle ${cycle}: Back position wrong. Expected ${initialPosition}, got ${backPos}`);
    }
  }

  // Final verification
  const finalPosition = await waitlist.getPosition(TARGET_USER);
  if (finalPosition !== initialPosition) {
    throw new Error(`Final position wrong. Expected ${initialPosition}, got ${finalPosition}`);
  }

  // Verify total length hasn't changed
  const finalLength = await waitlist.getLength();
  if (finalLength !== TOTAL_USERS) {
    throw new Error(`Expected ${TOTAL_USERS} users, got ${finalLength}`);
  }
}

async function testBatchInsertAndSequentialMoves(): Promise<void> {
  const TOTAL_USERS = 2000;
  const BATCH_SIZE = 400;
  const MOVE_START = 500;
  const MOVE_END = 1500;
  const TARGET_POSITION = 10;

  // Insert users in concurrent batches of 400
  const batches = [];
  for (let batchStart = 0; batchStart < TOTAL_USERS; batchStart += BATCH_SIZE) {
    const batchOperations = Array.from({ length: BATCH_SIZE }, (_, i) => {
      const userId = `user${batchStart + i}`;
      return waitlist.insertUser(userId, {
        email: `${userId}@test.com`,
        metadata: { name: `User ${batchStart + i}` }
      });
    });
    batches.push(Promise.all(batchOperations));
  }
  await Promise.all(batches);

  // Move users one by one to position 10
  for (let i = MOVE_START; i <= MOVE_END; i++) {
    const userId = `user${i}`;
    await waitlist.moveUser(userId, TARGET_POSITION);

    // Each user should end up at position TARGET_POSITION
    const pos = await waitlist.getPosition(userId);
    if (pos !== TARGET_POSITION) {
      const order = await waitlist.getOrderedIds();
      throw new Error(
        `User ${userId} at wrong position. Expected ${TARGET_POSITION}, got ${pos}. ` +
        `Users at positions 8-12: ${order.slice(7, 12).join(', ')}`
      );
    }
  }
}

// Main test runner
async function runTests() {
  let hasError = false;
  
  try {
    const tests = [
      ['Concurrent Email Users', testConcurrentEmailUsers],
      ['Concurrent Duplicate User Email', testConcurrentDuplicateUserEmail],
      ['Concurrent Duplicate User Phone', testConcurrentDuplicateUserPhone],
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
      ['Delete and Bump Up', testDeleteAndBumpUp],
      ['Limit and Concurrent Operations', testLimitAndConcurrentOperations],
      ['Large Scale Insert and Length', testLargeScaleInsertAndLength],
      ['Concurrent Delete and Bump', testConcurrentDeleteAndBump],
      ['Basic Invite Code Flow', testBasicInviteCodeFlow],
      ['Large Scale Invite Codes', testLargeScaleInviteCodes],
      ['Invite Code Limits', testInviteCodeLimits],
      ['Concurrent Invite Code Use', testConcurrentInviteCodeUse],
      ['Invite Code Position Edge Cases', testInviteCodePositionEdgeCases],
      ['Complex Concurrent Operations', testComplexConcurrentOperations],
      ['Repeated Large Scale Movement', testRepeatedLargeScaleMovement],
      ['Batch Insert and Sequential Moves', testBatchInsertAndSequentialMoves],
      ['Large Scale Backward Bump', testLargeScaleBackwardBump]
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