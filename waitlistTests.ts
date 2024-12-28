import dotenv from 'dotenv';
import { WaitlistManager } from './waitlistManager';
import { TrackedUser, InsertResult } from './waitlistTypes';

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

async function testConcurrentEmailUsers(): Promise<void> {
  // Store the results to get the IDs
  const insertResults: InsertResult[] = [];
  const operations = Array.from({ length: 20 }, (_, i) => {
    return waitlist.insertUser({
      email: `user${i}@test.com`,
      metadata: { name: `User ${i}` }
    });
  });

  const results = await Promise.all(operations);
  insertResults.push(...results);
  
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
    insertResults.map(result => waitlist.getPosition(result.id))
  );
  if (!finalPositions.every(p => p > 0 && p <= 20)) {
    throw new Error(`Final positions invalid: ${finalPositions.join(', ')}`);
  }
}

async function testConcurrentDuplicateUserEmail(): Promise<void> {
  const operations = Array.from({ length: 10 }, () => {
    return waitlist.insertUser({
      email: 'duplicate@test.com',
      metadata: { name: 'Duplicate User' }
    });
  });

  const results = await Promise.all(operations);
  const firstResult = results[0];  // Store the first result which contains our user's ID
  const position = firstResult.position;
  const allSamePosition = results.every(r => r.position === position);
  const newInsertions = results.filter(r => !r.already_existed);

  if (newInsertions.length !== 1) {
    throw new Error('Expected only one new insertion');
  }
  if (!allSamePosition) {
    throw new Error('All operations did not return the same position');
  }

  const finalPosition = await waitlist.getPosition(firstResult.id);  // Use the stored ID
  if (finalPosition !== position) {
    throw new Error(`Final position does not match: ${finalPosition} !== ${position}`);
  }
}

async function testConcurrentDuplicateUserPhone() {
  // Create initial user with phone and store the ID
  const phone = '+15551234567';
  const initialResult = await waitlist.insertUser({
    phone,
    metadata: { name: 'Original User' }
  });

  // Try to insert multiple users with the same phone concurrently
  const attempts = 5;
  const operations = [];
  
  for (let i = 0; i < attempts; i++) {
    operations.push(waitlist.insertUser({
      phone,
      metadata: { name: `Duplicate User ${i + 1}` }
    }));
  }

  // Run all operations concurrently
  const results = await Promise.all(operations.map(p => p.catch(e => e)));

  // Verify all attempts either returned the same position or failed
  const position = await waitlist.getPosition(initialResult.id);  // Use the stored ID
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
  // Insert 20 users sequentially and store their IDs
  const userIds: string[] = [];
  for (let i = 0; i < 20; i++) {
    const result = await waitlist.insertUser({
      email: `user${i}@test.com`,
      metadata: { name: `User ${i}` }
    });
    userIds.push(result.id);
  }

  // Move user 8 to position 4
  await waitlist.moveUser(userIds[8], 4);
  
  // Get actual order
  const actualOrder = await waitlist.getOrderedIds();

  // Verify user8's new position
  const user8Position = await waitlist.getPosition(userIds[8]);
  if (user8Position !== 4) {
    throw new Error(`Expected user8 to be at position 4, but found at position ${user8Position}`);
  }

  // Verify the exact order of first 5 positions
  const expectedStart = [userIds[0], userIds[1], userIds[2], userIds[8], userIds[3]];
  const actualStart = actualOrder.slice(0, 5);
  
  if (JSON.stringify(actualStart) !== JSON.stringify(expectedStart)) {
    throw new Error(
      `Expected order: ${expectedStart.join(', ')}\n` +
      `Actual order: ${actualStart.join(', ')}`
    );
  }
}

async function testLargeScaleBumpUp(): Promise<void> {
  // First verify Redis is clean
  const initialCount = await waitlist.getOrderedIds();
  if (initialCount.length !== 0) {
    throw new Error(`Redis not clean at start. Found ${initialCount.length} entries`);
  }

  // Insert 2000 users and store their IDs
  const users: TrackedUser[] = [];
  const insertOperations = Array.from({ length: 2000 }, (_, i) => {
    return waitlist.insertUser({
      email: `mass_user${i}@test.com`,
      metadata: { name: `Mass User ${i}` }
    }).then(result => {
      users[i] = { id: result.id, email: `mass_user${i}@test.com` };
      return result;
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
    await waitlist.moveUser(users[i].id, i - 99);
  }

  // Get final count and order
  const finalOrder = await waitlist.getOrderedIds();
  if (finalOrder.length !== 2000) {
    throw new Error(`Expected 2000 users, found ${finalOrder.length}`);
  }

  // Verify no duplicate IDs
  const uniqueIds = new Set(finalOrder);
  if (uniqueIds.size !== 2000) {
    throw new Error(`Found ${finalOrder.length} total entries but only ${uniqueIds.size} unique IDs`);
  }

  // Verify all expected users exist
  for (const user of users) {
    if (!uniqueIds.has(user.id)) {
      throw new Error(`Missing user: ${user.email}`);
    }
  }
}

async function testRepeatedBumpUp(): Promise<void> {
  // Insert 20 users sequentially and store their IDs
  const users: TrackedUser[] = [];
  for (let i = 0; i < 20; i++) {
    const result = await waitlist.insertUser({
      email: `bump_user${i}@test.com`,
      metadata: { name: `Bump User ${i}` }
    });
    users.push({ id: result.id, email: `bump_user${i}@test.com` });
  }

  // Move users to their final positions in reverse order
  for (let i = 0; i < 20; i++) {
    const userToMove = 19 - i;
    const targetPos = i + 1;
    await waitlist.moveUser(users[userToMove].id, targetPos);
  }

  // Get final order
  const finalOrder = await waitlist.getOrderedIds();

  // Verify exact final order (19 down to 0)
  const expectedOrder = users.slice().reverse().map(u => u.id);
  if (JSON.stringify(finalOrder) !== JSON.stringify(expectedOrder)) {
    throw new Error(
      'Expected reversed order of users\n' +
      `Got unexpected order`
    );
  }

  // Verify email mappings match reversed order
  for (let i = 0; i < finalOrder.length; i++) {
    const userId = finalOrder[i];
    const user = users.find(u => u.id === userId);
    if (!user) {
      throw new Error(`Could not find user with ID ${userId}`);
    }
    const mappedId = await waitlist.getEmailMapping(user.email!);
    if (mappedId !== userId) {
      throw new Error(`Email mapping at position ${i + 1} incorrect. Expected ${userId}, got ${mappedId}`);
    }
  }
}

async function testStepByStepBumpUp(): Promise<void> {
  // Insert 20 users sequentially and store their IDs
  const users: TrackedUser[] = [];
  for (let i = 0; i < 20; i++) {
    const result = await waitlist.insertUser({
      email: `bump_user${i}@test.com`,
      metadata: { name: `Bump User ${i}` }
    });
    users.push({ id: result.id, email: `bump_user${i}@test.com` });
  }

  // Get the ID of user19 that we'll be moving
  const user19 = users[19];

  // Move user19 up one position at a time
  for (let targetPos = 19; targetPos >= 1; targetPos--) {
    await waitlist.moveUser(user19.id, targetPos);
    const currentOrder = await waitlist.getOrderedIds();

    // Verify position after each move
    const currentPosition = await waitlist.getPosition(user19.id);
    if (currentPosition !== targetPos) {
      throw new Error(`Expected user19 at position ${targetPos}, but found at ${currentPosition}`);
    }

    // Verify the array is correct up to this point
    const expectedOrder = Array.from({ length: 20 }, (_, i) => {
      if (i < targetPos - 1) return users[i].id;
      if (i === targetPos - 1) return user19.id;
      if (i <= 18) return users[i-1].id;
      return users[18].id;
    });

    if (JSON.stringify(currentOrder) !== JSON.stringify(expectedOrder)) {
      throw new Error(
        `At target position ${targetPos}:\n` +
        `Expected order incorrect`
      );
    }
  }

  // Verify final state
  const finalOrder = await waitlist.getOrderedIds();

  // Final position should be 1 (1-based)
  const finalPosition = await waitlist.getPosition(user19.id);
  if (finalPosition !== 1) {
    throw new Error(`Expected user19 at position 1, but found at ${finalPosition}`);
  }

  // Expected final order: [19,0,1,2,3,...,18]
  const expectedFinalOrder = [user19.id].concat(
    users.slice(0, 19).map(u => u.id)
  );
  
  if (JSON.stringify(finalOrder) !== JSON.stringify(expectedFinalOrder)) {
    throw new Error('Final order is incorrect');
  }

  // Verify email mappings match final order
  for (let i = 0; i < finalOrder.length; i++) {
    const userId = finalOrder[i];
    const user = users.find(u => u.id === userId);
    if (!user) {
      throw new Error(`Could not find user with ID ${userId}`);
    }
    const mappedId = await waitlist.getEmailMapping(user.email!);
    if (mappedId !== userId) {
      throw new Error(`Email mapping at position ${i + 1} incorrect. Expected ${userId}, got ${mappedId}`);
    }
  }
}

async function testConcurrentBumpToSamePosition(): Promise<void> {
  // Insert 20 users sequentially and track their IDs
  const users: TrackedUser[] = [];
  for (let i = 0; i < 20; i++) {
    const result = await waitlist.insertUser({
      email: `bump_user${i}@test.com`,
      metadata: { name: `Bump User ${i}` }
    });
    users.push({ id: result.id, email: `bump_user${i}@test.com` });
  }

  // Concurrently move users 16-19 to position 2
  await Promise.all([
    waitlist.moveUser(users[16].id, 2),
    waitlist.moveUser(users[17].id, 2),
    waitlist.moveUser(users[18].id, 2),
    waitlist.moveUser(users[19].id, 2)
  ]);

  // Get final order
  const finalOrder = await waitlist.getOrderedIds();

  // Verify the structure:
  // 1. First position should be user0
  if (finalOrder[0] !== users[0].id) {
    throw new Error('First position should be user0');
  }

  // 2. Next four positions should contain users 16-19 in reverse order
  const movedUsers = finalOrder.slice(1, 5).map(id => 
    users.findIndex(u => u.id === id)
  );
  const expectedMovedUsers = [19, 18, 17, 16];
  if (JSON.stringify(movedUsers) !== JSON.stringify(expectedMovedUsers)) {
    throw new Error(
      'Positions 2-5 should contain users 19,18,17,16 in that order\n' +
      `Got: ${movedUsers.join(',')}`
    );
  }

  // 3. Remaining positions should be 1-15 in order
  const remainingUsers = finalOrder.slice(5).map(id => 
    users.findIndex(u => u.id === id)
  );
  const expectedRemaining = Array.from({ length: 15 }, (_, i) => i + 1);
  if (JSON.stringify(remainingUsers) !== JSON.stringify(expectedRemaining)) {
    throw new Error(
      'Remaining positions should be users 1-15 in order\n' +
      `Expected: ${expectedRemaining.join(',')}\n` +
      `Got: ${remainingUsers.join(',')}`
    );
  }

  // Verify email mappings match final order
  for (let i = 0; i < finalOrder.length; i++) {
    const userId = finalOrder[i];
    const user = users.find(u => u.id === userId);
    if (!user) {
      throw new Error(`Could not find user with ID ${userId}`);
    }
    const mappedId = await waitlist.getEmailMapping(user.email!);
    if (mappedId !== userId) {
      throw new Error(`Email mapping at position ${i + 1} incorrect. Expected ${userId}, got ${mappedId}`);
    }
  }
}

async function testPhoneAttachment(): Promise<void> {
  // Insert 20 users with emails only and track their IDs
  const users: TrackedUser[] = [];
  for (let i = 0; i < 20; i++) {
    const result = await waitlist.insertUser({
      email: `user${i}@test.com`,
      metadata: { name: `User ${i}` }
    });
    users.push({ id: result.id, email: `user${i}@test.com` });
  }

  // Attach phone numbers to all users
  const attachResults = await Promise.all(
    users.map((user, i) => 
      waitlist.attachPhone(user.id, `+1555000${i.toString().padStart(4, '0')}`)
    )
  );

  if (!attachResults.every(result => result === true)) {
    throw new Error('Not all phone attachments succeeded');
  }

  // Verify phone mappings
  for (let i = 0; i < 20; i++) {
    const user = users[i];
    const phone = `+1555000${i.toString().padStart(4, '0')}`;
    const mappedId = await waitlist.getPhoneMapping(phone);
    
    if (mappedId !== user.id) {
      throw new Error(`Phone mapping incorrect for ${user.id}. Expected ${user.id}, got ${mappedId}`);
    }
  }

  // Try to attach an already used phone number
  const duplicateResult = await waitlist.attachPhone(users[0].id, `+15550000001`);
  if (duplicateResult !== false) {
    throw new Error('Should not be able to attach already used phone number');
  }

  const originalMapping = await waitlist.getPhoneMapping('+15550000001');
  if (originalMapping !== users[1].id) {
    throw new Error(`Original phone mapping was affected. Expected ${users[1].id}, got ${originalMapping}`);
  }
}

async function testEmailAttachment(): Promise<void> {
  // Insert 20 users with phone numbers only and track their IDs
  const users: TrackedUser[] = [];
  for (let i = 0; i < 20; i++) {
    const result = await waitlist.insertUser({
      phone: `+1555000${i.toString().padStart(4, '0')}`,
      metadata: { name: `User ${i}` }
    });
    users.push({ id: result.id, phone: `+1555000${i.toString().padStart(4, '0')}` });
  }

  // Attach emails to all users
  const attachResults = await Promise.all(
    users.map((user, i) => 
      waitlist.attachEmail(user.id, `user${i}@test.com`)
    )
  );

  if (!attachResults.every(result => result === true)) {
    throw new Error('Not all email attachments succeeded');
  }

  // Verify email mappings
  for (let i = 0; i < 20; i++) {
    const user = users[i];
    const email = `user${i}@test.com`;
    const mappedId = await waitlist.getEmailMapping(email);
    
    if (mappedId !== user.id) {
      throw new Error(`Email mapping incorrect for ${user.id}. Expected ${user.id}, got ${mappedId}`);
    }
  }

  // Try to attach an already used email
  const duplicateResult = await waitlist.attachEmail(users[0].id, 'user1@test.com');
  if (duplicateResult !== false) {
    throw new Error('Should not be able to attach already used email');
  }

  const originalMapping = await waitlist.getEmailMapping('user1@test.com');
  if (originalMapping !== users[1].id) {
    throw new Error(`Original email mapping was affected. Expected ${users[1].id}, got ${originalMapping}`);
  }
}

async function testLargeListBumpToTop(): Promise<void> {
  // Insert first 5K users in parallel
  const users: TrackedUser[] = [];
  let insertOperations = Array.from({ length: 5000 }, (_, i) => {
    return waitlist.insertUser({
      email: `user${i}@test.com`,
      metadata: { name: `User ${i}` }
    }).then(result => {
      users[i] = { id: result.id, email: `user${i}@test.com` };
      return result;
    });
  });
  await Promise.all(insertOperations);

  // Insert second 5K users in parallel
  insertOperations = Array.from({ length: 5000 }, (_, i) => {
    const userNum = i + 5000;
    return waitlist.insertUser({
      email: `user${userNum}@test.com`,
      metadata: { name: `User ${userNum}` }
    }).then(result => {
      users[userNum] = { id: result.id, email: `user${userNum}@test.com` };
      return result;
    });
  });
  await Promise.all(insertOperations);

  // Move last user to top
  const lastUser = users[9999];
  await waitlist.moveUser(lastUser.id, 1);

  // Check that last user is now first
  const topPosition = await waitlist.getPosition(lastUser.id);
  if (topPosition !== 1) {
    throw new Error(`Expected ${lastUser.id} at position 1, found at ${topPosition}`);
  }

  // Get final order and verify it's correct
  const finalOrder = await waitlist.getOrderedIds();
  
  if (finalOrder.length !== 10000) {
    throw new Error(`Expected 10000 users, found ${finalOrder.length}`);
  }

  // Verify order: last user should be first, rest should be in original order
  const expectedOrder = [lastUser.id].concat(
    users.slice(0, 9999).map(u => u.id)
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
      const user = users.find(u => u.id === userId);
      if (!user) {
        throw new Error(`Could not find user info for ID ${userId}`);
      }
      return waitlist.getEmailMapping(user.email!).then(mappedId => {
        if (mappedId !== userId) {
          throw new Error(`Email mapping incorrect for ${userId}. Expected ${userId}, got ${mappedId}`);
        }
      });
    });

    await Promise.all(verificationPromises);
  }
}

async function testDeleteAndReinsert(): Promise<void> {
  // Insert 20 users sequentially and track their IDs
  const users: TrackedUser[] = [];
  for (let i = 0; i < 20; i++) {
    const result = await waitlist.insertUser({
      email: `user${i}@test.com`,
      phone: `+1555${i.toString().padStart(4, '0')}`,
      metadata: { name: `User ${i}` }
    });
    users.push({ 
      id: result.id, 
      email: `user${i}@test.com`,
      phone: `+1555${i.toString().padStart(4, '0')}`
    });
  }

  // Delete all users one by one
  for (const user of users) {
    const success = await waitlist.deleteUser(user.id);
    if (!success) {
      throw new Error(`Failed to delete ${user.id}`);
    }
  }

  // Verify list is empty
  const remainingIds = await waitlist.getOrderedIds();
  if (remainingIds.length !== 0) {
    throw new Error(`Expected empty list, but found ${remainingIds.length} users`);
  }

  // Verify email and phone mappings are cleared
  for (const user of users) {
    const emailMapping = await waitlist.getEmailMapping(user.email!);
    const phoneMapping = await waitlist.getPhoneMapping(user.phone!);
    
    if (emailMapping !== null) {
      throw new Error(`Email mapping still exists for ${user.email}`);
    }
    if (phoneMapping !== null) {
      throw new Error(`Phone mapping still exists for ${user.phone}`);
    }
  }

  // Re-insert 20 new users and track their IDs
  const newUsers: TrackedUser[] = [];
  for (let i = 0; i < 20; i++) {
    const result = await waitlist.insertUser({
      email: `new_user${i}@test.com`,
      phone: `+1666${i.toString().padStart(4, '0')}`,
      metadata: { name: `New User ${i}` }
    });
    newUsers.push({
      id: result.id,
      email: `new_user${i}@test.com`,
      phone: `+1666${i.toString().padStart(4, '0')}`
    });

    // Verify position is correct
    if (result.position !== i + 1) {
      throw new Error(`Expected new_user${i} at position ${i + 1}, got ${result.position}`);
    }
  }

  // Final verification of all positions
  for (let i = 0; i < 20; i++) {
    const position = await waitlist.getPosition(newUsers[i].id);
    if (position !== i + 1) {
      throw new Error(`Expected ${newUsers[i].id} at position ${i + 1}, got ${position}`);
    }
  }
}

async function testDeleteByEmailAndPhone(): Promise<void> {
  // Insert 20 users sequentially and track their IDs
  const users: TrackedUser[] = [];
  for (let i = 0; i < 20; i++) {
    const result = await waitlist.insertUser({
      email: `user${i}@test.com`,
      phone: `+1555${i.toString().padStart(4, '0')}`,
      metadata: { name: `User ${i}` }
    });
    users.push({
      id: result.id,
      email: `user${i}@test.com`,
      phone: `+1555${i.toString().padStart(4, '0')}`
    });
  }

  // Delete user5 by email and user6 by phone
  const deleteEmailResult = await waitlist.deleteUserByEmail(users[5].email!);
  if (!deleteEmailResult) {
    throw new Error('Failed to delete user5 by email');
  }

  const deletePhoneResult = await waitlist.deleteUserByPhone(users[6].phone!);
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

    const position = await waitlist.getPosition(users[i].id);
    const expectedPosition = i < 5 ? i + 1 : i - 1;
    
    if (position !== expectedPosition) {
      throw new Error(`Wrong position for ${users[i].id}: expected ${expectedPosition}, got ${position}`);
    }
  }

  // Verify deleted users' mappings are removed
  const email5Mapping = await waitlist.getEmailMapping(users[5].email!);
  const phone5Mapping = await waitlist.getPhoneMapping(users[5].phone!);
  const email6Mapping = await waitlist.getEmailMapping(users[6].email!);
  const phone6Mapping = await waitlist.getPhoneMapping(users[6].phone!);

  if (email5Mapping !== null || phone5Mapping !== null) {
    throw new Error('User5 mappings still exist');
  }
  if (email6Mapping !== null || phone6Mapping !== null) {
    throw new Error('User6 mappings still exist');
  }

  // Verify remaining users' mappings are intact
  for (let i = 0; i < 20; i++) {
    if (i === 5 || i === 6) continue;

    const user = users[i];
    const emailMapping = await waitlist.getEmailMapping(user.email!);
    const phoneMapping = await waitlist.getPhoneMapping(user.phone!);
    
    if (emailMapping !== user.id) {
      throw new Error(`Email mapping incorrect for ${user.id}`);
    }
    if (phoneMapping !== user.id) {
      throw new Error(`Phone mapping incorrect for ${user.id}`);
    }
  }
}

async function testDeleteAndBumpUp(): Promise<void> {
  // Insert 20 users sequentially and track their IDs
  const users: TrackedUser[] = [];
  for (let i = 0; i < 20; i++) {
    const result = await waitlist.insertUser({
      email: `user${i}@test.com`,
      phone: `+1555${i.toString().padStart(4, '0')}`,
      metadata: { name: `User ${i}` }
    });
    users.push({
      id: result.id,
      email: `user${i}@test.com`,
      phone: `+1555${i.toString().padStart(4, '0')}`
    });
  }

  // Delete user5 by email and user6 by phone
  const deleteEmailResult = await waitlist.deleteUserByEmail(users[5].email!);
  if (!deleteEmailResult) {
    throw new Error('Failed to delete user5 by email');
  }

  const deletePhoneResult = await waitlist.deleteUserByPhone(users[6].phone!);
  if (!deletePhoneResult) {
    throw new Error('Failed to delete user6 by phone');
  }

  // Move last user (user19) to position 3
  await waitlist.moveUser(users[19].id, 3);

  // Get final order and verify count
  const finalOrder = await waitlist.getOrderedIds();
  if (finalOrder.length !== 18) {
    throw new Error(`Expected 18 users, but found ${finalOrder.length}`);
  }

  // Expected order: user0, user1, user19, user2, user3, user4, user7, user8, ..., user18
  const expectedOrder = [
    users[0].id, users[1].id, users[19].id, users[2].id, users[3].id, users[4].id,
    ...users.slice(7, 19).map(u => u.id)
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
    const user = users.find(u => u.id === userId)!;
    const emailMapping = await waitlist.getEmailMapping(user.email!);
    const phoneMapping = await waitlist.getPhoneMapping(user.phone!);
    
    if (emailMapping !== userId) {
      throw new Error(`Email mapping incorrect for ${userId}`);
    }
    if (phoneMapping !== userId) {
      throw new Error(`Phone mapping incorrect for ${userId}`);
    }
  }

  // Verify deleted users' mappings are removed
  const deletedMappings = await Promise.all([
    waitlist.getEmailMapping(users[5].email!),
    waitlist.getPhoneMapping(users[5].phone!),
    waitlist.getEmailMapping(users[6].email!),
    waitlist.getPhoneMapping(users[6].phone!)
  ]);

  if (deletedMappings.some(mapping => mapping !== null)) {
    throw new Error('Some mappings for deleted users still exist');
  }
}

async function testLimitAndConcurrentOperations(): Promise<void> {
  try {
    // Set a small limit
    waitlist.setListLimit(5);

    // Try to insert 10 users concurrently and track their IDs
    const operations = Array.from({ length: 10 }, async (_, i) => {
      return waitlist.insertUser({
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
    waitlist.setListLimit(100000);
  }
}

async function testLargeScaleInsertAndLength(): Promise<void> {
  const TOTAL_USERS = 50000;
  const BATCH_SIZE = 5000;
  const NUM_BATCHES = TOTAL_USERS / BATCH_SIZE;

  // Track all users
  const users: TrackedUser[] = new Array(TOTAL_USERS);

  // Insert users in batches
  for (let batch = 0; batch < NUM_BATCHES; batch++) {
    const batchStart = batch * BATCH_SIZE;
    
    const batchOperations = Array.from({ length: BATCH_SIZE }, (_, i) => {
      const userIndex = batchStart + i;
      return waitlist.insertUser({
        email: `mass_user${userIndex}@test.com`,
        metadata: { name: `Mass User ${userIndex}` }
      });
    });

    const results = await Promise.all(batchOperations);
    
    // Store results after batch completes
    results.forEach((result, i) => {
      const userIndex = batchStart + i;
      users[userIndex] = { 
        id: result.id, 
        email: `mass_user${userIndex}@test.com` 
      };
    });
  }

  // Verify final length
  const finalLength = await waitlist.getLength();
  if (finalLength !== TOTAL_USERS) {
    throw new Error(`Expected length ${TOTAL_USERS}, but got ${finalLength}`);
  }

  // Verify random sample of users exist (checking all 50K would be too slow)
  const sampleSize = 100;
  const sampleIndices = Array.from({ length: sampleSize }, 
    () => Math.floor(Math.random() * TOTAL_USERS)
  );

  await Promise.all(
    sampleIndices.map(async index => {
      const user = users[index];
      const position = await waitlist.getPosition(user.id);
      if (!position) {
        throw new Error(`Sample user ${user.id} not found`);
      }
    })
  );
}

async function testConcurrentDeleteAndBump(): Promise<void> {
  // Insert 21 users sequentially and track their IDs
  const users: TrackedUser[] = [];
  for (let i = 0; i <= 20; i++) {
    const result = await waitlist.insertUser({
      email: `user${i}@test.com`,
      phone: `+1555${i.toString().padStart(4, '0')}`,
      metadata: { name: `User ${i}` }
    });
    users.push({
      id: result.id,
      email: `user${i}@test.com`,
      phone: `+1555${i.toString().padStart(4, '0')}`
    });
  }

  // Test concurrent operations: delete 4-7 and bump 17-20 to those positions
  const operations = [
    // Delete operations
    waitlist.deleteUser(users[4].id),
    waitlist.deleteUser(users[5].id),
    waitlist.deleteUser(users[6].id),
    waitlist.deleteUser(users[7].id),
    // Bump operations
    waitlist.moveUser(users[17].id, 4),
    waitlist.moveUser(users[18].id, 5),
    waitlist.moveUser(users[19].id, 6),
    waitlist.moveUser(users[20].id, 7)
  ];

  // Run operations concurrently
  await Promise.all(operations);

  // Verify deleted users are gone
  for (let i = 4; i <= 7; i++) {
    const position = await waitlist.getPosition(users[i].id);
    if (position !== 0) {
      throw new Error(`User ${users[i].id} should be deleted but found at position ${position}`);
    }
  }

  // Verify bumped users are in correct positions
  const expectedPositions = {
    [users[17].id]: 4,
    [users[18].id]: 5,
    [users[19].id]: 6,
    [users[20].id]: 7
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
    users[0].id, users[1].id, users[2].id,
    users[17].id, users[18].id, users[19].id, users[20].id,
    users[3].id,  // user3 gets pushed back after the bumped users
    users[8].id, users[9].id, users[10].id, users[11].id, users[12].id,
    users[13].id, users[14].id, users[15].id, users[16].id
  ];

  if (JSON.stringify(finalOrder) !== JSON.stringify(expectedOrder)) {
    throw new Error(
      'Incorrect final order.\n' +
      `Expected: ${expectedOrder.join(', ')}\n` +
      `Got: ${finalOrder.join(', ')}`
    );
  }
}

async function testBasicInviteCodeFlow(): Promise<void> {
  // Create initial user
  const creatorResult = await waitlist.insertUser({
    email: 'creator1@test.com',
    metadata: { name: 'Creator 1' }
  });

  // Create invite code
  const code = await waitlist.createInviteCode(creatorResult.id, 3);
  
  // Verify code exists and creator
  const creator = await waitlist.getInviteCodeCreator(code);
  if (!creator) {
    throw new Error('Invite code was not stored properly');
  }
  if (creator !== creatorResult.id) {
    throw new Error(`Wrong creator for code. Expected ${creatorResult.id}, got ${creator}`);
  }

  // Verify bump positions
  const bumpPos = await waitlist.getInviteCodeBumpPositions(code);
  if (bumpPos !== 3) {
    throw new Error(`Wrong bump positions. Expected 3, got ${bumpPos}`);
  }
  
  // Use invite code
  const invitedResult = await waitlist.useInviteCode(code, {
    email: 'invited1@test.com',
    metadata: { name: 'Invited User 1' }
  }, 0);

  // Verify positions
  const creatorPosition = await waitlist.getPosition(creatorResult.id);
  if (creatorPosition !== 1) {
    throw new Error(`Expected creator at position 1, got ${creatorPosition}`);
  }

  if (invitedResult.position !== 2) {
    throw new Error(`Expected invited user at position 2, got ${invitedResult.position}`);
  }
}

async function testLargeScaleInviteCodes(): Promise<void> {
  const CREATORS = 100;
  const CODES_PER_CREATOR = 3;
  const TOTAL_USERS = CREATORS * 2;

  // Create creators and store their results (including IDs)
  const creators: TrackedUser[] = [];
  const creatorResults = await Promise.all(
    Array.from({ length: CREATORS }, async (_, i) => {
      const result = await waitlist.insertUser({
        email: `creator${i}@test.com`,
        metadata: { name: `Creator ${i}` }
      });
      creators.push({ id: result.id, email: `creator${i}@test.com` });
      return result;
    })
  );

  // Generate codes (300 codes total)
  const codes: { creatorEmail: string, creatorId: string, code: string }[] = [];
  for (const creator of creators) {
    for (let j = 0; j < CODES_PER_CREATOR; j++) {
      const code = await waitlist.createInviteCode(creator.id, j + 1);
      codes.push({ 
        creatorEmail: creator.email!, 
        creatorId: creator.id, 
        code 
      });
    }
  }

  // Track invited users' IDs
  const invitedUsers: TrackedUser[] = [];

  // Use one code per creator (100 uses total)
  const useOperations = codes.slice(0, CREATORS).map(async (codeInfo, i) => {
    const result = await waitlist.useInviteCode(
      codeInfo.code,
      {
        email: `invited${i}@test.com`,
        metadata: { name: `Invited User ${i}` }
      },
      i % 3
    ).catch(error => {
      console.error(`Failed to use code ${codeInfo.code}:`, error.message);
      throw error;
    });
    invitedUsers.push({ id: result.id, email: `invited${i}@test.com` });
    return result;
  });

  const results = await Promise.allSettled(useOperations);
  const failures = results.filter(r => r.status === 'rejected');
  
  if (failures.length > 0) {
    console.error(`${failures.length} invite code uses failed:`);
    failures.forEach(f => console.error((f as PromiseRejectedResult).reason));
    throw new Error(`Failed to use ${failures.length} invite codes`);
  }

  // Verify final state using stored IDs
  const finalLength = await waitlist.getLength();
  
  if (finalLength !== TOTAL_USERS) {
    const allUsers = await waitlist.getOrderedIds();
    console.log('All users:', allUsers);
    throw new Error(`Expected ${TOTAL_USERS} users, got ${finalLength}`);
  }

  // Verify all users exist using stored IDs
  for (const creator of creators) {
    const creatorExists = await waitlist.getPosition(creator.id);
    if (!creatorExists) {
      throw new Error(`Creator ${creator.email} not found`);
    }
  }

  for (const invited of invitedUsers) {
    const invitedExists = await waitlist.getPosition(invited.id);
    if (!invitedExists) {
      throw new Error(`Invited user ${invited.email} not found`);
    }
  }

  // Verify email mappings
  for (const user of [...creators, ...invitedUsers]) {
    const mappedId = await waitlist.getEmailMapping(user.email!);
    if (mappedId !== user.id) {
      throw new Error(`Email mapping incorrect for ${user.email}. Expected ${user.id}, got ${mappedId}`);
    }
  }

  // Verify invite code creators are properly stored
  for (const codeInfo of codes.slice(0, CREATORS)) {
    const storedCreator = await waitlist.getInviteCodeCreator(codeInfo.code);
    if (storedCreator !== codeInfo.creatorId) {
      throw new Error(`Invite code creator mismatch for code ${codeInfo.code}`);
    }
  }
}

async function testInviteCodeLimits(): Promise<void> {
  // Create user
  const limitTesterResult = await waitlist.insertUser({
    email: 'limit@test.com',
    metadata: { name: 'Limit Tester' }
  });

  // Set low invite code limit
  waitlist.setInviteCodeLimit(2);

  // Try to create more codes than allowed
  const code1 = await waitlist.createInviteCode(limitTesterResult.id, 1);
  const code2 = await waitlist.createInviteCode(limitTesterResult.id, 1);
  
  try {
    await waitlist.createInviteCode(limitTesterResult.id, 1);
    throw new Error('Should not allow creating more than limit');
  } catch (e) {
    if (!(e instanceof Error) || !e.message.includes('Invite code limit reached')) {
      throw e;
    }
  }

  // Verify existing codes still work
  const invited1Result = await waitlist.useInviteCode(code1, {
    email: 'invited1@test.com',
    metadata: { name: 'Invited 1' }
  }, 1);

  const invited2Result = await waitlist.useInviteCode(code2, {
    email: 'invited2@test.com',
    metadata: { name: 'Invited 2' }
  }, 1);

  // Verify positions
  const positions = await Promise.all([
    waitlist.getPosition(limitTesterResult.id),
    waitlist.getPosition(invited1Result.id),
    waitlist.getPosition(invited2Result.id)
  ]);

  if (!positions.every(p => p > 0)) {
    throw new Error('Some users not found in waitlist');
  }

  // Reset limit
  waitlist.setInviteCodeLimit(3);
}

async function testConcurrentInviteCodeUse(): Promise<void> {
  // Create initial user and store ID
  const creatorResult = await waitlist.insertUser({
    email: 'multi@test.com',
    metadata: { name: 'Multi Creator' }
  });

  // Create a single invite code
  const code = await waitlist.createInviteCode(creatorResult.id, 2);

  // Try to use same code concurrently
  const attempts = 5;
  const operations = Array.from({ length: attempts }, (_, i) =>
    waitlist.useInviteCode(
      code,
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
  // Insert 20 users sequentially and track their IDs
  const users: TrackedUser[] = [];
  for (let i = 0; i < 20; i++) {
    const result = await waitlist.insertUser({
      email: `user${i}@test.com`,
      phone: `+1555${i.toString().padStart(4, '0')}`,
      metadata: { name: `User ${i}` }
    });
    users.push({
      id: result.id,
      email: `user${i}@test.com`,
      phone: `+1555${i.toString().padStart(4, '0')}`
    });
  }

  // Create invite code from user15 with large bump (100)
  const largeCode = await waitlist.createInviteCode(users[15].id, 100);
  
  // Use the code and verify actual position
  const invited1Result = await waitlist.useInviteCode(largeCode, {
    email: 'invited1@test.com',
    metadata: { name: 'Invited User 1' }
  }, 0);

  // Verify user15 moved to position 1
  const user15NewPos = await waitlist.getPosition(users[15].id);
  if (user15NewPos !== 1) {
    throw new Error(`Expected user15 at position 1, got ${user15NewPos}`);
  }

  // Get user10's new position after user15's move
  const user10PosAfterFirst = await waitlist.getPosition(users[10].id);

  // Create another code with small bump (2) and test position prediction
  const smallCode = await waitlist.createInviteCode(users[10].id, 2);
  const smallPredictedPos = await waitlist.getPositionAfterInviteCodeUse(smallCode);
  
  // Calculate expected position based on current position and bump amount
  const expectedPos = Math.max(1, user10PosAfterFirst - 2);
  
  if (smallPredictedPos !== expectedPos) {
    throw new Error(`Expected predicted position ${expectedPos}, got ${smallPredictedPos}`);
  }

  // Use the small bump code
  const invited2Result = await waitlist.useInviteCode(smallCode, {
    email: 'invited2@test.com',
    metadata: { name: 'Invited User 2' }
  }, 0);

  // Verify final position
  const finalUser10Pos = await waitlist.getPosition(users[10].id);
  if (finalUser10Pos !== expectedPos) {
    throw new Error(`Expected user10 at position ${expectedPos}, got ${finalUser10Pos}`);
  }

  // Verify final order
  const finalOrder = await waitlist.getOrderedIds();
  if (finalOrder[0] !== users[15].id || finalOrder[expectedPos - 1] !== users[10].id) {
    throw new Error('Final order is incorrect');
  }
}

async function testComplexConcurrentOperations(): Promise<void> {
  // Setup initial users (20 users) and track their IDs
  const users: TrackedUser[] = [];
  for (let i = 0; i < 20; i++) {
    const result = await waitlist.insertUser({
      email: `base${i}@test.com`,
      metadata: { name: `Base User ${i}` }
    });
    users.push({ id: result.id, email: `base${i}@test.com` });
  }

  interface InviteCodeInfo {
    code: string;
    creator: TrackedUser;
    bumpPositions: number;
  }

  // Create invite codes with different bump positions
  const inviteCodes: InviteCodeInfo[] = [];
  for (let i = 0; i < 5; i++) {
    // Use users that won't be deleted (indices 0,2,3,5,6 since we delete 1,4,7...)
    const safeIndices = [0, 2, 3, 5, 6];
    const creator = users[safeIndices[i]];
    const code = await waitlist.createInviteCode(creator.id, i + 1);
    inviteCodes.push({ code, creator, bumpPositions: i + 1 });
  }

  // Track which users will be deleted (every 3rd user starting from index 1)
  const toDelete = users.filter((_, i) => (i - 1) % 3 === 0);

  // Track new and invited users
  const newUsers: TrackedUser[] = [];
  const invitedUsers: TrackedUser[] = [];

  // Prepare concurrent operations
  const operations = [
    // 5 deletes (every 3rd user starting from base1)
    ...toDelete.map(user => waitlist.deleteUser(user.id)),

    // 10 new inserts
    ...Array.from({ length: 10 }, (_, i) =>
      waitlist.insertUser({
        email: `new${i}@test.com`,
        metadata: { name: `New User ${i}` }
      }).then(result => {
        newUsers.push({ id: result.id, email: `new${i}@test.com` });
        return result;
      })),

    // 5 invite code uses
    ...inviteCodes.map((invite, i) =>
      waitlist.useInviteCode(
        invite.code,
        {
          email: `invited${i}@test.com`,
          metadata: { name: `Invited User ${i}` }
        },
        invite.bumpPositions
      ).then(result => {
        invitedUsers.push({ id: result.id, email: `invited${i}@test.com` });
        return result;
      }))
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
    // Original users that weren't deleted
    ...users.filter(u => !toDelete.find(d => d.id === u.id)),
    ...newUsers,
    ...invitedUsers
  ];

  for (const user of shouldExist) {
    const exists = await waitlist.getPosition(user.id);
    if (!exists) {
      throw new Error(`Expected user ${user.email} not found`);
    }
  }

  // Verify deleted users don't exist
  for (const user of toDelete) {
    const exists = await waitlist.getPosition(user.id);
    if (exists) {
      throw new Error(`Deleted user ${user.email} still exists at position ${exists}`);
    }
  }

  // Verify invite code creators are in correct positions relative to their invitees
  for (const { creator, bumpPositions } of inviteCodes) {
    // Skip if creator was deleted
    if (toDelete.find(d => d.id === creator.id)) continue;

    const creatorPos = await waitlist.getPosition(creator.id);
    
    // Find invited user's position
    const invitedUser = invitedUsers[inviteCodes.findIndex(ic => ic.creator.id === creator.id)];
    const invitedPos = await waitlist.getPosition(invitedUser.id);
    
    if (creatorPos >= invitedPos) {
      throw new Error(
        `Creator ${creator.email} at position ${creatorPos} should be ahead of invited user ${invitedUser.email} at position ${invitedPos}`
      );
    }
  }
}

async function testRepeatedLargeScaleMovement(): Promise<void> {
  const TOTAL_USERS = 10000;
  const BATCH_SIZE = 5;  // 2000 batches of 5 users each
  const MOVEMENT_CYCLES = 100;

  // Insert users in batches and track their IDs
  const users: TrackedUser[] = new Array(TOTAL_USERS);
  
  for (let batchStart = 0; batchStart < TOTAL_USERS; batchStart += BATCH_SIZE) {
    const batchOperations = Array.from({ length: BATCH_SIZE }, (_, i) => {
      const userIndex = batchStart + i;
      return waitlist.insertUser({
        email: `user${userIndex}@test.com`,
        metadata: { name: `User ${userIndex}` }
      });
    });
    
    const results = await Promise.all(batchOperations);
    results.forEach((result, i) => {
      const userIndex = batchStart + i;
      users[userIndex] = { 
        id: result.id, 
        email: `user${userIndex}@test.com` 
      };
    });
  }

  // Get target user (user5000)
  const targetUser = users[5000];

  // Record initial position
  const initialPosition = await waitlist.getPosition(targetUser.id);
  if (initialPosition !== 5001) { // 0-based to 1-based indexing
    throw new Error(`Initial position wrong. Expected 5001, got ${initialPosition}`);
  }

  // Move user back and forth repeatedly
  for (let cycle = 0; cycle < MOVEMENT_CYCLES; cycle++) {
    // Move to position 1
    await waitlist.moveUser(targetUser.id, 1);
    const frontPos = await waitlist.getPosition(targetUser.id);
    if (frontPos !== 1) {
      throw new Error(`Cycle ${cycle}: Front position wrong. Expected 1, got ${frontPos}`);
    }

    // Move back to original position
    await waitlist.moveUser(targetUser.id, initialPosition);
    const backPos = await waitlist.getPosition(targetUser.id);
    if (backPos !== initialPosition) {
      throw new Error(`Cycle ${cycle}: Back position wrong. Expected ${initialPosition}, got ${backPos}`);
    }
  }

  // Final verification
  const finalPosition = await waitlist.getPosition(targetUser.id);
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
  const batches: Promise<InsertResult[]>[] = [];
  for (let batchStart = 0; batchStart < TOTAL_USERS; batchStart += BATCH_SIZE) {
    const batchOperations = Array.from({ length: BATCH_SIZE }, (_, i) => {
      const userId = `user${batchStart + i}`;
      return waitlist.insertUser({
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

async function testLargeScaleBackwardBump(): Promise<void> {
  const TOTAL_USERS = 2000;
  const BUMP_START = 400;
  const BUMP_END = 1000;
  const BUMP_POSITIONS = 99;

  // Insert users sequentially and track their IDs
  const users: TrackedUser[] = new Array(TOTAL_USERS);
  const insertOperations = Array.from({ length: TOTAL_USERS }, (_, i) =>
    waitlist.insertUser({
      email: `user${i}@test.com`,
      metadata: { name: `User ${i}` }
    }).then(result => {
      users[i] = { id: result.id, email: `user${i}@test.com` };
      return result;
    })
  );
  await Promise.all(insertOperations);

  // Move users backward one at a time (to higher numbers)
  for (let i = BUMP_END; i >= BUMP_START; i--) {  // Move in REVERSE order
    const targetPos = Math.min(TOTAL_USERS, i + 1 + BUMP_POSITIONS);
    await waitlist.moveUser(users[i].id, targetPos);
  }

  // Verify final positions
  for (let i = BUMP_START; i <= BUMP_END; i++) {
    const pos = await waitlist.getPosition(users[i].id);
    const expectedPos = Math.min(TOTAL_USERS, i + 1 + BUMP_POSITIONS);
    
    if (pos !== expectedPos) {
      throw new Error(`User ${users[i].email} at wrong position. Expected ${expectedPos}, got ${pos}`);
    }
  }

  // Verify total length hasn't changed
  const finalLength = await waitlist.getLength();
  if (finalLength !== TOTAL_USERS) {
    throw new Error(`Expected ${TOTAL_USERS} users, got ${finalLength}`);
  }
}

async function testInviteCodeFormat(): Promise<void> {
  // Create initial user
  const creatorResult = await waitlist.insertUser({
    email: 'creator@test.com',
    metadata: { name: 'Creator' }
  });

  // Test default length (6)
  const defaultCode = await waitlist.createInviteCode(creatorResult.id, 1);
  if (defaultCode.length !== 6) {
    throw new Error(`Expected code length 6, got ${defaultCode.length}`);
  }
  if (!/^[A-Z0-9]+$/.test(defaultCode)) {
    throw new Error(`Code contains invalid characters: ${defaultCode}`);
  }

  // Test setting new length
  waitlist.setInviteCodeLength(10);
  const longerCode = await waitlist.createInviteCode(creatorResult.id, 1);
  if (longerCode.length !== 10) {
    throw new Error(`Expected code length 10, got ${longerCode.length}`);
  }
  if (!/^[A-Z0-9]+$/.test(longerCode)) {
    throw new Error(`Code contains invalid characters: ${longerCode}`);
  }

  // Test length limits
  try {
    waitlist.setInviteCodeLength(16);
    throw new Error('Should not allow length > 15');
  } catch (e) {
    if (!(e instanceof Error) || !e.message.includes('cannot exceed 15')) {
      throw e;
    }
  }

  try {
    waitlist.setInviteCodeLength(0);
    throw new Error('Should not allow length < 1');
  } catch (e) {
    if (!(e instanceof Error) || !e.message.includes('must be at least 1')) {
      throw e;
    }
  }
}

async function testInviteCodeLengthChanges(): Promise<void> {
  // Create initial user
  const creatorResult = await waitlist.insertUser({
    email: 'creator@test.com',
    metadata: { name: 'Creator' }
  });

  // Test default length (6)
  const defaultCode = await waitlist.createInviteCode(creatorResult.id, 1);
  if (defaultCode.length !== 6) {
    throw new Error(`Default code length wrong. Expected 6, got ${defaultCode.length}`);
  }

  // Change to length 8
  waitlist.setInviteCodeLength(8);
  const code8 = await waitlist.createInviteCode(creatorResult.id, 1);
  if (code8.length !== 8) {
    throw new Error(`Code length wrong after change. Expected 8, got ${code8.length}`);
  }

  // Change to length 4
  waitlist.setInviteCodeLength(4);
  const code4 = await waitlist.createInviteCode(creatorResult.id, 1);
  if (code4.length !== 4) {
    throw new Error(`Code length wrong after second change. Expected 4, got ${code4.length}`);
  }

  // Verify all codes are alphanumeric uppercase
  const allCodes = [defaultCode, code8, code4];
  for (const code of allCodes) {
    if (!/^[A-Z0-9]+$/.test(code)) {
      throw new Error(`Invalid characters in code: ${code}`);
    }
  }
}

async function testInviteCodeTracking(): Promise<void> {
  // Create two users: a creator and someone who will use their code
  const creatorResult = await waitlist.insertUser({
    email: 'creator@test.com',
    phone: '+15551234567',
    metadata: { name: 'Creator' }
  });

  // Create multiple invite codes
  const code1 = await waitlist.createInviteCode(creatorResult.id, 1);
  const code2 = await waitlist.createInviteCode(creatorResult.id, 2);
  const code3 = await waitlist.createInviteCode(creatorResult.id, 3);

  // Verify getUserCreatedInviteCodes
  const createdCodes = await waitlist.getUserCreatedInviteCodes(creatorResult.id);
  if (createdCodes.length !== 3) {
    throw new Error(`Expected 3 created codes, got ${createdCodes.length}`);
  }
  
  const expectedCodes = [code1, code2, code3].sort();
  const actualCodes = [...createdCodes].sort();
  if (JSON.stringify(actualCodes) !== JSON.stringify(expectedCodes)) {
    throw new Error(`Created codes mismatch. Expected ${expectedCodes}, got ${actualCodes}`);
  }

  // Use one of the codes
  const invitedResult = await waitlist.useInviteCode(code2, {
    email: 'invited@test.com',
    phone: '+15557654321',
    metadata: { name: 'Invited User' }
  }, 2);

  // Check invite code usage info for the invited user
  const usageInfo = await waitlist.getInviteCodeUsedBy(invitedResult.id);
  
  // Verify code
  if (usageInfo.code !== code2) {
    throw new Error(`Wrong invite code. Expected ${code2}, got ${usageInfo.code}`);
  }

  // Verify creator ID
  if (usageInfo.creatorId !== creatorResult.id) {
    throw new Error(`Wrong creator ID. Expected ${creatorResult.id}, got ${usageInfo.creatorId}`);
  }

  // Verify creator email
  if (usageInfo.creatorEmail !== 'creator@test.com') {
    throw new Error(`Wrong creator email. Expected creator@test.com, got ${usageInfo.creatorEmail}`);
  }

  // Verify creator phone
  if (usageInfo.creatorPhone !== '+15551234567') {
    throw new Error(`Wrong creator phone. Expected +15551234567, got ${usageInfo.creatorPhone}`);
  }

  // Check invite code info for a user who didn't use an invite code
  const regularResult = await waitlist.insertUser({
    email: 'regular@test.com',
    metadata: { name: 'Regular User' }
  });

  const regularUsageInfo = await waitlist.getInviteCodeUsedBy(regularResult.id);
  if (regularUsageInfo.code !== null || 
      regularUsageInfo.creatorId !== null || 
      regularUsageInfo.creatorEmail !== null || 
      regularUsageInfo.creatorPhone !== null) {
    throw new Error('Expected null values for user who did not use invite code');
  }
}

async function testSignupCutoffChanges(): Promise<void> {
  // Insert 10 users
  const users: TrackedUser[] = [];
  for (let i = 0; i < 10; i++) {
    const result = await waitlist.insertUser({
      email: `user${i}@test.com`,
      metadata: { name: `User ${i}` }
    });
    users.push({ id: result.id, email: `user${i}@test.com` });
  }

  // Test setting cutoff higher than list length (should set to list length)
  waitlist.setSignupCutoff(20);
  await new Promise(resolve => setTimeout(resolve, 100));

  for (const user of users) {
    const canSignUp = await waitlist.canUserSignUp(user.id);
    if (!canSignUp) {
      throw new Error(`User ${user.email} should be able to sign up with high cutoff`);
    }
  }

  // Test setting cutoff to 5
  waitlist.setSignupCutoff(5);
  await new Promise(resolve => setTimeout(resolve, 100));

  for (let i = 0; i < users.length; i++) {
    const canSignUp = await waitlist.canUserSignUp(users[i].id);
    if (i < 5 && !canSignUp) {
      throw new Error(`User ${users[i].email} should be able to sign up (position ${i + 1} <= 5)`);
    }
    if (i >= 5 && canSignUp) {
      throw new Error(`User ${users[i].email} should not be able to sign up (position ${i + 1} > 5)`);
    }
  }

  // Test setting cutoff to -1 (no one can sign up)
  waitlist.setSignupCutoff(-1);
  await new Promise(resolve => setTimeout(resolve, 100));

  for (const user of users) {
    const canSignUp = await waitlist.canUserSignUp(user.id);
    if (canSignUp) {
      throw new Error(`User ${user.email} should not be able to sign up with cutoff -1`);
    }
  }
}

async function testSignupCutoffReduction(): Promise<void> {
  // Insert 10 users
  const users: TrackedUser[] = [];
  for (let i = 0; i < 10; i++) {
    const result = await waitlist.insertUser({
      email: `user${i}@test.com`,
      metadata: { name: `User ${i}` }
    });
    users.push({ id: result.id, email: `user${i}@test.com` });
  }

  // Set initial cutoff to 5 and wait a bit for it to be set
  waitlist.setSignupCutoff(5);
  await new Promise(resolve => setTimeout(resolve, 100));

  // Sign up first two users
  const signupResults = await Promise.all([
    waitlist.markUserAsSignedUp(users[0].id),
    waitlist.markUserAsSignedUp(users[1].id)
  ]);

  if (!signupResults[0] || !signupResults[1]) {
    throw new Error('Failed to sign up initial users');
  }

  // Reduce cutoff to 2 and wait a bit for it to be set
  waitlist.setSignupCutoff(2);
  await new Promise(resolve => setTimeout(resolve, 100));

  // Try to sign up user at position 4 (was in old cutoff, now outside)
  const user4SignupResult = await waitlist.markUserAsSignedUp(users[3].id);
  if (user4SignupResult) {
    throw new Error('User 4 should not be able to sign up after cutoff reduction');
  }

  // Verify user 4 still can't sign up
  const canUser4SignUp = await waitlist.canUserSignUp(users[3].id);
  if (canUser4SignUp) {
    throw new Error('User 4 should not be allowed to sign up');
  }
}

async function testSignupCutoffDeletion(): Promise<void> {
  // Insert 10 users
  const users: TrackedUser[] = [];
  for (let i = 0; i < 10; i++) {
    const result = await waitlist.insertUser({
      email: `user${i}@test.com`,
      phone: `+1555${i.toString().padStart(4, '0')}`,
      metadata: { name: `User ${i}` }
    });
    users.push({ 
      id: result.id, 
      email: `user${i}@test.com`,
      phone: `+1555${i.toString().padStart(4, '0')}`
    });
  }

  // Set cutoff to 5 and wait for it to be set
  waitlist.setSignupCutoff(5);
  await new Promise(resolve => setTimeout(resolve, 100));

  // Sign up first two users
  await Promise.all([
    waitlist.markUserAsSignedUp(users[0].id),
    waitlist.markUserAsSignedUp(users[1].id)
  ]);

  // Try to delete users in different positions
  // Should fail for signed up users (0,1)
  // Should succeed for users in cutoff but not signed up (2,3,4)
  // Should succeed for users outside cutoff (5+)
  for (let i = 0; i < users.length; i++) {
    try {
      await waitlist.deleteUser(users[i].id);
      if (i <= 1) {
        throw new Error(`Should not be able to delete signed up user ${i}`);
      }
      // Success is expected for all other users (both within and outside cutoff)
    } catch (e) {
      if (i > 1) {
        throw new Error(`Should be able to delete user ${i}`);
      }
      if (!(e instanceof Error) || !e.message.includes('Cannot delete user that has already signed up')) {
        throw e;
      }
    }
  }

  // Verify signed up users still exist
  for (let i = 0; i < 2; i++) {
    const position = await waitlist.getPosition(users[i].id);
    if (!position || position !== i + 1) {
      throw new Error(`Signed up user ${i} should still exist at position ${i + 1}`);
    }
  }

  // Verify other users were deleted
  for (let i = 2; i < users.length; i++) {
    const position = await waitlist.getPosition(users[i].id);
    if (position !== 0) {
      throw new Error(`User ${i} should have been deleted`);
    }
  }
}

async function testSignupCutoffMovement(): Promise<void> {
  // Insert 10 users
  const users: TrackedUser[] = [];
  for (let i = 0; i < 10; i++) {
    const result = await waitlist.insertUser({
      email: `user${i}@test.com`,
      metadata: { name: `User ${i}` }
    });
    users.push({ id: result.id, email: `user${i}@test.com` });
  }

  // Set cutoff to 10 so we can sign up users
  waitlist.setSignupCutoff(10);
  await new Promise(resolve => setTimeout(resolve, 100));

  // Sign up first two users
  await Promise.all([
    waitlist.markUserAsSignedUp(users[0].id),
    waitlist.markUserAsSignedUp(users[1].id)
  ]);

  // Set cutoff to 5 for the movement tests
  waitlist.setSignupCutoff(5);
  await new Promise(resolve => setTimeout(resolve, 100));

  // Try to move users in different scenarios
  const testCases = [
    // Try to move signed up user (position 1)
    { user: users[0], targetPos: 3, error: 'Cannot move user that has already signed up' },
    
    // Try to move user in cutoff (position 3) up
    { user: users[2], targetPos: 2, error: 'Cannot move user that is within signup cutoff' },
    
    // Try to move user in cutoff (position 3) down
    { user: users[2], targetPos: 7, error: 'Cannot move user that is within signup cutoff' },
    
    // Try to move user outside cutoff (position 7) into cutoff
    { user: users[6], targetPos: 3, error: 'Cannot move user to position at or above signup cutoff' },
    
    // Verify user outside cutoff can move to another position outside cutoff
    { user: users[6], targetPos: 8, error: null }
  ];

  for (const testCase of testCases) {
    try {
      await waitlist.moveUser(testCase.user.id, testCase.targetPos);
      if (testCase.error) {
        throw new Error(`Expected error "${testCase.error}" but move succeeded for user ${testCase.user.email}`);
      }
    } catch (e) {
      if (!testCase.error) {
        if (e instanceof Error) {
          throw new Error(`Expected move to succeed for user ${testCase.user.email} but got error: ${e.message}`);
        }
        throw e;
      }
      if (!(e instanceof Error) || !e.message.includes(testCase.error)) {
        if (e instanceof Error) {
          throw new Error(`Expected error "${testCase.error}" but got "${e.message}" for user ${testCase.user.email}`);
        }
        throw e;
      }
    }
  }

  // Verify positions haven't changed for users in cutoff
  for (let i = 0; i < 5; i++) {
    const position = await waitlist.getPosition(users[i].id);
    if (position !== i + 1) {
      throw new Error(`User ${i} should still be at position ${i + 1}, but is at ${position}`);
    }
  }
}

async function testSignedUpUserMovement(): Promise<void> {
  // Insert 10 users
  const users: TrackedUser[] = [];
  for (let i = 0; i < 10; i++) {
    const result = await waitlist.insertUser({
      email: `user${i}@test.com`,
      metadata: { name: `User ${i}` }
    });
    users.push({ id: result.id, email: `user${i}@test.com` });
  }

  // Set cutoff to 10 so we can sign up users
  waitlist.setSignupCutoff(10);
  await new Promise(resolve => setTimeout(resolve, 100));

  // Sign up users 1 and 3
  await Promise.all([
    waitlist.markUserAsSignedUp(users[0].id),
    waitlist.markUserAsSignedUp(users[2].id)
  ]);

  // Set cutoff back to 5 for the movement tests
  waitlist.setSignupCutoff(5);
  await new Promise(resolve => setTimeout(resolve, 100));

  // Try to move signed up users
  const testCases = [
    // Try to move first signed up user up and down
    { user: users[0], targetPos: 2, error: 'Cannot move user that has already signed up' },
    { user: users[0], targetPos: 5, error: 'Cannot move user that has already signed up' },
    
    // Try to move second signed up user up and down
    { user: users[2], targetPos: 1, error: 'Cannot move user that has already signed up' },
    { user: users[2], targetPos: 7, error: 'Cannot move user that has already signed up' }
  ];

  for (const testCase of testCases) {
    try {
      await waitlist.moveUser(testCase.user.id, testCase.targetPos);
      throw new Error(`Expected error for moving signed up user ${testCase.user.email} to position ${testCase.targetPos}`);
    } catch (e) {
      if (!(e instanceof Error) || !e.message.includes(testCase.error)) {
        throw e;
      }
    }
  }

  // Verify positions haven't changed
  const finalPositions = await Promise.all([
    waitlist.getPosition(users[0].id),
    waitlist.getPosition(users[2].id)
  ]);

  if (finalPositions[0] !== 1) {
    throw new Error(`First signed up user should still be at position 1, but is at ${finalPositions[0]}`);
  }
  if (finalPositions[1] !== 3) {
    throw new Error(`Second signed up user should still be at position 3, but is at ${finalPositions[1]}`);
  }
}

async function testSignupCutoffBoundary(): Promise<void> {
  // Insert 10 users
  const users: TrackedUser[] = [];
  for (let i = 0; i < 10; i++) {
    const result = await waitlist.insertUser({
      email: `user${i}@test.com`,
      metadata: { name: `User ${i}` }
    });
    users.push({ id: result.id, email: `user${i}@test.com` });
  }

  // Set cutoff to 5
  waitlist.setSignupCutoff(5);

  // Try to move users from outside the cutoff into various positions within it
  const testCases = [
    // Try to move to first position
    { user: users[7], targetPos: 1, error: 'Cannot move user to position at or above signup cutoff' },
    
    // Try to move to middle of cutoff
    { user: users[8], targetPos: 3, error: 'Cannot move user to position at or above signup cutoff' },
    
    // Try to move to cutoff boundary
    { user: users[9], targetPos: 5, error: 'Cannot move user to position at or above signup cutoff' },
    
    // Verify can move to position just after cutoff
    { user: users[7], targetPos: 6, error: null }
  ];

  for (const testCase of testCases) {
    try {
      await waitlist.moveUser(testCase.user.id, testCase.targetPos);
      if (testCase.error) {
        throw new Error(`Expected error "${testCase.error}" but move succeeded for user ${testCase.user.email}`);
      }
    } catch (e) {
      if (!testCase.error) {
        if (e instanceof Error) {
          throw new Error(`Expected move to succeed for user ${testCase.user.email} but got error: ${e.message}`);
        }
        throw e;
      }
      if (!(e instanceof Error) || !e.message.includes(testCase.error)) {
        if (e instanceof Error) {
          throw new Error(`Expected error "${testCase.error}" but got "${e.message}" for user ${testCase.user.email}`);
        }
        throw e;
      }
    }
  }

  // Verify positions of users in cutoff haven't changed
  for (let i = 0; i < 5; i++) {
    const position = await waitlist.getPosition(users[i].id);
    if (position !== i + 1) {
      throw new Error(`User ${i} should still be at position ${i + 1}, but is at ${position}`);
    }
  }
}

async function testSignedUpUserDeletion(): Promise<void> {
  // Insert 10 users
  const users: TrackedUser[] = [];
  for (let i = 0; i < 10; i++) {
    const result = await waitlist.insertUser({
      email: `user${i}@test.com`,
      phone: `+1555000${i.toString().padStart(4, '0')}`,
      metadata: { name: `User ${i}` }
    });
    users.push({ 
      id: result.id, 
      email: `user${i}@test.com`,
      phone: `+1555000${i.toString().padStart(4, '0')}`
    });
  }

  // Set cutoff to 10 so we can sign up users
  await waitlist.setSignupCutoff(10);

  // Sign up users at positions 1, 4, and 7
  await Promise.all([
    waitlist.markUserAsSignedUp(users[0].id),
    waitlist.markUserAsSignedUp(users[3].id),
    waitlist.markUserAsSignedUp(users[6].id)
  ]);

  // Set cutoff back to 5 for the deletion tests
  await waitlist.setSignupCutoff(5);

  // Verify users are actually signed up
  const signedUpStates = await Promise.all([
    waitlist.isUserSignedUp(users[0].id),
    waitlist.isUserSignedUp(users[3].id),
    waitlist.isUserSignedUp(users[6].id)
  ]);

  if (!signedUpStates.every(state => state)) {
    throw new Error('Failed to mark users as signed up');
  }

  // Try to delete signed up users using all deletion methods
  const signedUpIndices = [0, 3, 6];
  for (const index of signedUpIndices) {
    const user = users[index];
    const testCases = [
      { method: () => waitlist.deleteUser(user.id), desc: 'deleteUser' },
      { method: () => waitlist.deleteUserByEmail(user.email!), desc: 'deleteUserByEmail' },
      { method: () => waitlist.deleteUserByPhone(user.phone!), desc: 'deleteUserByPhone' }
    ];

    for (const testCase of testCases) {
      try {
        await testCase.method();
        throw new Error(`Expected deletion to fail for signed up user when using ${testCase.desc}`);
      } catch (e) {
        if (!(e instanceof Error)) {
          throw e;
        }
        if (!(
          e.message.includes('Cannot delete user that has already signed up') ||
          e.message.includes('Cannot delete user within signup cutoff')
        )) {
          throw e; // Re-throw if it's not one of our expected error messages
        }
        // Otherwise the error is expected, continue to next test case
      }
    }
  }

  // Verify signed up users still exist in their original positions
  const positions = await Promise.all([
    waitlist.getPosition(users[0].id),
    waitlist.getPosition(users[3].id),
    waitlist.getPosition(users[6].id)
  ]);

  if (positions[0] !== 1) {
    throw new Error(`First signed up user should still be at position 1, but is at ${positions[0]}`);
  }
  if (positions[1] !== 4) {
    throw new Error(`Second signed up user should still be at position 4, but is at ${positions[1]}`);
  }
  if (positions[2] !== 7) {
    throw new Error(`Third signed up user should still be at position 7, but is at ${positions[2]}`);
  }

  // Verify we can still delete non-signed up users
  await waitlist.deleteUser(users[8].id);
  const deletedPosition = await waitlist.getPosition(users[8].id);
  if (deletedPosition !== 0) {
    throw new Error(`Deleted user should not exist in waitlist`);
  }
}

async function testSignupAllUsers(): Promise<void> {
  // Insert 10 users
  const users: TrackedUser[] = [];
  for (let i = 0; i < 10; i++) {
    const result = await waitlist.insertUser({
      email: `user${i}@test.com`,
      metadata: { name: `User ${i}` }
    });
    users.push({ id: result.id, email: `user${i}@test.com` });
  }

  // Set cutoff to list length (10)
  await waitlist.setSignupCutoff(10);
  await new Promise(resolve => setTimeout(resolve, 100)); // Wait for cutoff to be set

  // Try to sign up all users
  const signupResults = await Promise.all(
    users.map(user => waitlist.markUserAsSignedUp(user.id))
  );

  // Verify all signups succeeded
  for (let i = 0; i < users.length; i++) {
    if (!signupResults[i]) {
      throw new Error(`Failed to sign up user ${users[i].email}`);
    }
  }

  // Double check that all users are marked as signed up
  const signupStates = await Promise.all(
    users.map(user => waitlist.isUserSignedUp(user.id))
  );

  for (let i = 0; i < users.length; i++) {
    if (!signupStates[i]) {
      throw new Error(`User ${users[i].email} should be marked as signed up`);
    }
  }
}

async function testCodeUsageWithWaitlistedUsers(): Promise<void> {
  // Add 20 users to waitlist
  const users: TrackedUser[] = [];
  for (let i = 0; i < 20; i++) {
    const result = await waitlist.insertUser({
      email: `user${i}@test.com`,
      metadata: { name: `User ${i}` }
    });
    users.push({ id: result.id, email: `user${i}@test.com` });
  }

  // Create code with 10 uses
  await waitlist.createCommunityCode('TEST10', 10);

  // Use code with first 5 users
  for (let i = 0; i < 5; i++) {
    const result = await waitlist.useCommunityCode('TEST10', {
      email: users[i].email
    });
    if (result !== true) {
      throw new Error(`Failed to use code for user ${i}: ${result}`);
    }
  }

  // Verify code info
  const codeInfo = await waitlist.getCommunityCodeInfo('TEST10');
  if (!codeInfo || codeInfo.remainingUses !== 5) {
    throw new Error(`Expected 5 remaining uses, got ${codeInfo?.remainingUses}`);
  }

  // Verify users are marked as signed up
  for (let i = 0; i < 5; i++) {
    const isSignedUp = await waitlist.isUserSignedUp(users[i].id);
    if (!isSignedUp) {
      throw new Error(`User ${i} should be marked as signed up`);
    }
  }
}

async function testConcurrentCodeUsageWithSameUser(): Promise<void> {
  // Add 20 users
  const users: TrackedUser[] = [];
  for (let i = 0; i < 20; i++) {
    const result = await waitlist.insertUser({
      email: `user${i}@test.com`,
      metadata: { name: `User ${i}` }
    });
    users.push({ id: result.id, email: `user${i}@test.com` });
  }

  // Create code
  await waitlist.createCommunityCode('CONCURRENT', 10);

  type CodeResult = 
  | { success: true; result: true | string }
  | { success: false; error: string };

  // Try to use code 5 times concurrently with same user
  const operations = Array(5).fill(null).map(() => 
    waitlist.useCommunityCode('CONCURRENT', { email: users[0].email })
      .then(result => ({ success: true, result } as CodeResult))
      .catch(error => ({ success: false, error: error.toString() } as CodeResult))
  );

  const results = await Promise.all(operations);
  
  // Count successes and failures
  const successes = results.filter((r): r is { success: true; result: true } => 
    r.success && r.result === true
  ).length;

  const failures = results.filter(r => 
    !r.success || (r.success && r.result !== true)
  ).length;

  if (successes !== 1 || failures !== 4) {
    throw new Error(`Expected 1 success and 4 failures, got ${successes} successes and ${failures} failures`);
  }

  // Verify code was only used once
  const codeInfo = await waitlist.getCommunityCodeInfo('CONCURRENT');
  if (!codeInfo || codeInfo.currentUses !== 1) {
    throw new Error(`Expected 1 code use, got ${codeInfo?.currentUses}`);
  }

  // Verify user is marked as signed up
  const isSignedUp = await waitlist.isUserSignedUp(users[0].id);
  if (!isSignedUp) {
    throw new Error('User should be marked as signed up');
  }
}

async function testCodeUsageLimit(): Promise<void> {
  // Add 21 users
  const users: TrackedUser[] = [];
  for (let i = 0; i < 21; i++) {
    const result = await waitlist.insertUser({
      email: `user${i}@test.com`,
      metadata: { name: `User ${i}` }
    });
    users.push({ id: result.id, email: `user${i}@test.com` });
  }

  // Create code with 20 uses
  await waitlist.createCommunityCode('LIMIT20', 20);

  type CodeResult = 
  | { success: true; result: true | string }
  | { success: false; error: string };

  // Use code with first 20 users
  for (let i = 0; i < 20; i++) {
    const result = await waitlist.useCommunityCode('LIMIT20', { email: users[i].email })
      .then(result => ({ success: true, result } as CodeResult))
      .catch(error => ({ success: false, error: error.toString() } as CodeResult));
    
    if (!result.success || result.result !== true) {
      throw new Error(`Expected success for user ${i}, got: ${result.success ? result.result : result.error}`);
    }
  }

  // Try to use code with 21st user - should get usage limit message
  const result = await waitlist.useCommunityCode('LIMIT20', { email: users[20].email })
    .then(result => ({ success: true, result } as CodeResult))
    .catch(error => ({ success: false, error: error.toString() } as CodeResult));

  if (result.success && result.result === true) {
    throw new Error('Expected code usage to fail, but it succeeded');
  }
  if (result.success && result.result !== 'Code has reached usage limit') {
    throw new Error(`Expected 'Code has reached usage limit', got: ${result.result}`);
  }
}

async function testCodeUsageWithNonWaitlistedUsers(): Promise<void> {
  // Create code with 10 uses
  await waitlist.createCommunityCode('NONWL10', 10);

  type CodeResult = 
  | { success: true; result: true | string }
  | { success: false; error: string };

  // Use code 10 times with non-waitlisted users
  for (let i = 0; i < 10; i++) {
    const result = await waitlist.useCommunityCode('NONWL10', {
      email: `nonwl${i}@test.com`
    })
      .then(result => ({ success: true, result } as CodeResult))
      .catch(error => ({ success: false, error: error.toString() } as CodeResult));
    
    if (!result.success || result.result !== true) {
      throw new Error(`Failed to use code for non-waitlisted user ${i}: ${result.success ? result.result : result.error}`);
    }
  }

  // Try to use code one more time
  const result = await waitlist.useCommunityCode('NONWL10', {
    email: 'extra@test.com'
  })
    .then(result => ({ success: true, result } as CodeResult))
    .catch(error => ({ success: false, error: error.toString() } as CodeResult));

  if (result.success && result.result === true) {
    throw new Error('Code should not work after reaching usage limit');
  }
  if (result.success && result.result !== 'Code has reached usage limit') {
    throw new Error(`Expected 'Code has reached usage limit', got: ${result.result}`);
  }
}

async function testConcurrentCodeUsageWithNonWaitlistedUsers(): Promise<void> {
  // Create code with 100 uses
  await waitlist.createCommunityCode('CONCURRENT100', 100);

  // Try to use code 110 times concurrently
  const operations = Array(110).fill(null).map((_, i) => 
    waitlist.useCommunityCode('CONCURRENT100', { email: `concurrent${i}@test.com` })
      .then(result => result === true)  // true for success
      .catch(() => false)  // false for any error
  );

  const results = await Promise.all(operations);
  
  // Count successes and failures
  const successes = results.filter(r => r === true).length;
  const failures = results.filter(r => r === false).length;

  if (successes !== 100 || failures !== 10) {
    throw new Error(`Expected 100 successes and 10 failures, got ${successes} successes and ${failures} failures`);
  }

  // Verify code is fully used
  const codeInfo = await waitlist.getCommunityCodeInfo('CONCURRENT100');
  if (!codeInfo || codeInfo.currentUses !== 100) {
    throw new Error(`Expected 100 code uses, got ${codeInfo?.currentUses}`);
  }
}

async function testConcurrentCodeUsageWithWaitlistedUsers(): Promise<void> {
  // Add 110 users
  const users: TrackedUser[] = [];
  for (let i = 0; i < 110; i++) {
    const result = await waitlist.insertUser({
      email: `user${i}@test.com`,
      metadata: { name: `User ${i}` }
    });
    users.push({ id: result.id, email: `user${i}@test.com` });
  }

  // Create code with 100 uses
  await waitlist.createCommunityCode('CONCURRENTWL100', 100);

  // Try to use code 110 times concurrently
  const operations = users.map(user => 
    waitlist.useCommunityCode('CONCURRENTWL100', { email: user.email })
      .then(result => result === true)  // true for success
      .catch(() => false)  // false for any error
  );

  const results = await Promise.all(operations);
  
  // Count successes and failures
  const successes = results.filter(r => r === true).length;
  const failures = results.filter(r => r === false).length;

  if (successes !== 100 || failures !== 10) {
    throw new Error(`Expected 100 successes and 10 failures, got ${successes} successes and ${failures} failures`);
  }

  // Verify code is fully used
  const codeInfo = await waitlist.getCommunityCodeInfo('CONCURRENTWL100');
  if (!codeInfo || codeInfo.currentUses !== 100) {
    throw new Error(`Expected 100 code uses, got ${codeInfo?.currentUses}`);
  }

  // Verify exactly 100 users are marked as signed up
  const signedUpCount = await Promise.all(users.map(u => waitlist.isUserSignedUp(u.id)))
    .then(results => results.filter(r => r).length);
  
  if (signedUpCount !== 100) {
    throw new Error(`Expected 100 signed up users, got ${signedUpCount}`);
  }
}

async function testVariousCommunityCodeCreation(): Promise<void> {
  // Array of test codes with different patterns and max uses
  const testCodes = [
    { code: '123', maxUses: 5 },                    // Short numeric
    { code: 'ABCDEF', maxUses: 10 },                // All letters
    { code: 'TEST100', maxUses: 100 },              // Mixed alphanumeric
    { code: '12345678', maxUses: 1 },               // Long numeric
    { code: 'LAUNCH2024', maxUses: 50 },            // Real-world example
    { code: 'ABC123XYZ', maxUses: 25 },             // Mixed pattern
    { code: '999999', maxUses: 999 },               // Repeated numbers
    { code: 'TESTCODE', maxUses: 15 },              // Common pattern
    { code: '123ABC', maxUses: 30 },                // Numbers then letters
    { code: 'ABC123', maxUses: 30 },                // Letters then numbers
    { code: 'A1B2C3', maxUses: 40 },                // Alternating
    { code: 'TEST', maxUses: 200 },                 // Short word
    { code: 'BETA555', maxUses: 75 },               // Word with numbers
    { code: '777WIN', maxUses: 60 },                // Numbers with word
    { code: 'TESTTEST', maxUses: 45 },              // Repeated word
    { code: '55TEST55', maxUses: 55 },              // Surrounded by numbers
    { code: 'CODE1234', maxUses: 80 },              // Word with sequence
    { code: '2024LAUNCH', maxUses: 150 },           // Year prefix
    { code: 'DEMO999', maxUses: 90 },               // Word with repeated number
    { code: 'TEST_2024', maxUses: 120 }             // With underscore
  ];

  // Create all codes
  for (const { code, maxUses } of testCodes) {
    const created = await waitlist.createCommunityCode(code, maxUses);
    if (!created) {
      throw new Error(`Failed to create code: ${code}`);
    }
  }

  // Verify all codes exist with correct max uses
  for (const { code, maxUses } of testCodes) {
    const info = await waitlist.getCommunityCodeInfo(code);
    if (!info) {
      throw new Error(`Code not found: ${code}`);
    }
    if (info.maxUses !== maxUses) {
      throw new Error(`Wrong max uses for ${code}: expected ${maxUses}, got ${info.maxUses}`);
    }
    if (info.currentUses !== 0) {
      throw new Error(`Code ${code} should have 0 uses initially, got ${info.currentUses}`);
    }
  }
}

async function testCommunityCodeDeletion(): Promise<void> {
  // Create 3 codes
  const codes = [
    { code: 'CODE1', maxUses: 10 },
    { code: 'CODE2', maxUses: 20 },
    { code: 'CODE3', maxUses: 30 }
  ];

  for (const { code, maxUses } of codes) {
    await waitlist.createCommunityCode(code, maxUses);
  }

  // Delete code 2
  const deleted = await waitlist.deleteCommunityCode('CODE2');
  if (!deleted) {
    throw new Error('Failed to delete CODE2');
  }

  // Verify only codes 1 and 3 remain
  const code1Info = await waitlist.getCommunityCodeInfo('CODE1');
  const code2Info = await waitlist.getCommunityCodeInfo('CODE2');
  const code3Info = await waitlist.getCommunityCodeInfo('CODE3');

  if (!code1Info || !code3Info || code2Info) {
    throw new Error('Expected only CODE1 and CODE3 to exist');
  }
}

async function testCommunityCodeDeletionWithSignedUpUsers(): Promise<void> {
  // Create 5 users in waitlist
  const users: TrackedUser[] = [];
  for (let i = 0; i < 5; i++) {
    const result = await waitlist.insertUser({
      email: `user${i}@test.com`,
      metadata: { name: `User ${i}` }
    });
    users.push({ id: result.id, email: `user${i}@test.com` });
  }

  // Create 2 codes
  await waitlist.createCommunityCode('CODE_A', 5);
  await waitlist.createCommunityCode('CODE_B', 5);

  // Have users use the codes
  await waitlist.useCommunityCode('CODE_A', { email: users[0].email });
  await waitlist.useCommunityCode('CODE_A', { email: users[1].email });
  await waitlist.useCommunityCode('CODE_B', { email: users[2].email });
  await waitlist.useCommunityCode('CODE_B', { email: users[3].email });

  // Delete both codes
  await waitlist.deleteCommunityCode('CODE_A');
  await waitlist.deleteCommunityCode('CODE_B');

  // Verify codes are gone
  const codeAInfo = await waitlist.getCommunityCodeInfo('CODE_A');
  const codeBInfo = await waitlist.getCommunityCodeInfo('CODE_B');
  if (codeAInfo || codeBInfo) {
    throw new Error('Codes should be deleted');
  }

  // Verify users are still signed up
  const signedUpStates = await Promise.all([
    waitlist.isUserSignedUp(users[0].id),
    waitlist.isUserSignedUp(users[1].id),
    waitlist.isUserSignedUp(users[2].id),
    waitlist.isUserSignedUp(users[3].id),
    waitlist.isUserSignedUp(users[4].id)
  ]);

  const expectedStates = [true, true, true, true, false];
  for (let i = 0; i < users.length; i++) {
    if (signedUpStates[i] !== expectedStates[i]) {
      throw new Error(`User ${i} signup state is wrong. Expected ${expectedStates[i]}, got ${signedUpStates[i]}`);
    }
  }
}

async function testBulkCommunityCodeDeletion(): Promise<void> {
  // Create 10 codes
  const codes = Array.from({ length: 10 }, (_, i) => ({
    code: `BULK${i + 1}`,
    maxUses: 10
  }));

  for (const { code, maxUses } of codes) {
    await waitlist.createCommunityCode(code, maxUses);
  }

  // Delete codes 1,3,5,7,9
  for (let i = 0; i < 10; i += 2) {
    await waitlist.deleteCommunityCode(`BULK${i + 1}`);
  }

  // Verify only even-numbered codes remain
  for (let i = 0; i < 10; i++) {
    const info = await waitlist.getCommunityCodeInfo(`BULK${i + 1}`);
    const shouldExist = i % 2 === 1; // Even indices (odd numbers) should exist

    if (shouldExist && !info) {
      throw new Error(`Code BULK${i + 1} should exist but doesn't`);
    }
    if (!shouldExist && info) {
      throw new Error(`Code BULK${i + 1} shouldn't exist but does`);
    }
  }
}

async function testConcurrentCommunityCodeDeletion(): Promise<void> {
  // Create a code
  await waitlist.createCommunityCode('CONCURRENT_DELETE', 100);

  // Try to delete the same code 10 times concurrently
  const operations = Array(10).fill(null).map(() => 
    waitlist.deleteCommunityCode('CONCURRENT_DELETE')
      .then(result => {
        return result;  // Already boolean
      })
      .catch(err => {
        console.error('Delete error:', err);  // Debug log
        return false;
      })
  );

  const results = await Promise.all(operations);
  
  const successes = results.filter(r => r === true).length;
  const failures = results.filter(r => r === false).length;

  if (successes !== 1 || failures !== 9) {
    throw new Error(`Expected 1 success and 9 failures, got ${successes} successes and ${failures} failures`);
  }

  // Verify code is actually deleted
  const codeInfo = await waitlist.getCommunityCodeInfo('CONCURRENT_DELETE');
  if (codeInfo !== null) {
    throw new Error('Code should be deleted but still exists');
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
      ['Large Scale Backward Bump', testLargeScaleBackwardBump],
      ['Invite Code Format', testInviteCodeFormat],
      ['Invite Code Length Changes', testInviteCodeLengthChanges],
      ['Invite Code Tracking', testInviteCodeTracking],
      ['Signup Cutoff Changes', testSignupCutoffChanges],
      ['Signup Cutoff Reduction', testSignupCutoffReduction],
      ['Signup Cutoff Deletion', testSignupCutoffDeletion],
      ['Signup Cutoff Movement', testSignupCutoffMovement],
      ['Signed Up User Movement', testSignedUpUserMovement],
      ['Signup Cutoff Boundary', testSignupCutoffBoundary],
      ['Signed Up User Deletion', testSignedUpUserDeletion],
      ['Sign Up All Users', testSignupAllUsers],
      ['Community Code Usage with Waitlisted Users', testCodeUsageWithWaitlistedUsers],
      ['Concurrent Community Code Usage with Same User', testConcurrentCodeUsageWithSameUser],
      ['Community Code Usage Limit', testCodeUsageLimit],
      ['Community Code Usage with Non-Waitlisted Users', testCodeUsageWithNonWaitlistedUsers],
      ['Concurrent Community Code Usage with Non-Waitlisted Users', testConcurrentCodeUsageWithNonWaitlistedUsers],
      ['Concurrent Community Code Usage with Waitlisted Users', testConcurrentCodeUsageWithWaitlistedUsers],
      ['Various Community Code Creation', testVariousCommunityCodeCreation],
      ['Community Code Deletion', testCommunityCodeDeletion],
      ['Community Code Deletion with Signed Up Users', testCommunityCodeDeletionWithSignedUpUsers],
      ['Bulk Community Code Deletion', testBulkCommunityCodeDeletion],
      ['Concurrent Community Code Deletion', testConcurrentCommunityCodeDeletion]
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