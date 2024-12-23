import dotenv from 'dotenv';
import { WaitlistManager } from './waitlistManager';

dotenv.config();

const waitlist = new WaitlistManager({
 host: process.env.REDIS_HOST || 'localhost',
 port: parseInt(process.env.REDIS_PORT || '6379'),
 password: process.env.REDIS_PASSWORD || '',
});

async function clearRedis(): Promise<void> {
 const redis = (waitlist as any).redis;
 await redis.flushdb();
}

async function forceCleanup(): Promise<void> {
 try {
   const redis = new WaitlistManager({
     host: process.env.REDIS_HOST || 'localhost',
     port: parseInt(process.env.REDIS_PORT || '6379'),
     password: process.env.REDIS_PASSWORD || '',
   });
   await clearRedis();
   await redis.disconnect();
 } catch (error) {
   console.error('Failed to force cleanup:', error);
 }
}

async function setup(): Promise<void> {
 try {
   await forceCleanup();
 } catch (error) {
   console.error('Setup cleanup failed:', error);
 }
}

async function testConcurrentEmailUsers(): Promise<void> {
 console.log('\nTesting concurrent email users insertion...');
 
 // Insert 20 users concurrently
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

 console.log(`Positions assigned: ${positions.join(', ')}`);
 console.log(`All unique positions: ${uniquePositions.size === 20}`);
 console.log(`Position range correct: ${Math.min(...positions) === 1 && Math.max(...positions) === 20}`);
 console.log(`No duplicates reported: ${alreadyExisted.every(existed => !existed)}`);

 const finalPositions = await Promise.all(
   Array.from({ length: 20 }, (_, i) => waitlist.getPosition(`user${i}`))
 );
 console.log(`Final positions verified: ${finalPositions.every(p => p > 0 && p <= 20)}`);
}

async function testConcurrentDuplicateUserEmail(): Promise<void> {
 console.log('\nTesting concurrent insertion of same user by email...');
 
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

 console.log(`Only one new insertion: ${newInsertions.length === 1}`);
 console.log(`All operations returned same position: ${allSamePosition}`);
 console.log(`Position returned: ${position}`);

 const finalPosition = await waitlist.getPosition('duplicate_user');
 console.log(`Final position matches: ${finalPosition === position}`);
}

async function testOrderedBumpUp(): Promise<void> {
 console.log('\nTesting ordered bump up operations...');
 
 // Insert 20 users sequentially
 for (let i = 0; i < 20; i++) {
   await waitlist.insertUser(`bump_user${i}`, {
     email: `bump_user${i}@test.com`,
     metadata: { name: `Bump User ${i}` }
   });
 }

 // Move user 8 to position 4
 console.log('\nMoving bump_user8 to position 4...');
 await waitlist.bumpUserUp('bump_user8', 4);
  
 // Get actual order
 const actualOrder = await waitlist._getOrderedIds();

 // Verify user8's new position
 const user8Position = await waitlist.getPosition('bump_user8');
 console.log('User 8 final position:', user8Position);
  
 if (user8Position !== 4) {
   throw new Error(`Expected user8 to be at position 4, but found at position ${user8Position}`);
 }

 // Verify the exact order of first 5 positions
 const expectedStart = ['bump_user0', 'bump_user1', 'bump_user2', 'bump_user8', 'bump_user3'];
 const actualStart = actualOrder.slice(0, 5);
  
 if (JSON.stringify(actualStart) !== JSON.stringify(expectedStart)) {
   throw new Error(`Expected order: ${expectedStart.join(', ')}\nActual order: ${actualStart.join(', ')}`);
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
 console.log('\nTesting large scale bump up operations...');
 
 // Insert 2000 users
 console.log('Inserting 2000 users...');
 const insertOperations = Array.from({ length: 2000 }, (_, i) => {
   return waitlist.insertUser(`mass_user${i}`, {
     email: `mass_user${i}@test.com`,
     metadata: { name: `Mass User ${i}` }
   });
 });

 await Promise.all(insertOperations);

 // Move users 500-1000 to positions 400-900
 console.log('Moving users 500-1000...');
 for (let i = 499; i < 1000; i++) {
   await waitlist.bumpUserUp(`mass_user${i}`, i - 99); // Move each 100 positions up
   if ((i + 1) % 100 === 0) {
     console.log(`Processed ${i + 1} users`);
   }
 }

 // Verify final positions
 console.log('Verifying positions...');
 const finalPositions = await Promise.all(
   Array.from({ length: 2000 }, (_, i) => waitlist.getPosition(`mass_user${i}`))
 );

 // Verify positions are correct
 for (let i = 0; i < 2000; i++) {
   const pos = finalPositions[i];
   if (pos === 0 || pos > 2000) {
     throw new Error(`Invalid position ${pos} for user ${i}`);
   }
 }

 // Verify email mappings match final positions
 console.log('Verifying email mappings match final positions...');
 const finalOrder = await waitlist._getOrderedIds();
 for (let i = 0; i < finalOrder.length; i++) {
   const userId = finalOrder[i];
   const email = `${userId}@test.com`;
   const mappedId = await waitlist._getEmailMapping(email);
   if (mappedId !== userId || mappedId !== finalOrder[i]) {
     throw new Error(`Email mapping at position ${i + 1} incorrect. Expected ${userId}, got ${mappedId}`);
   }
 }
}

async function testRepeatedBumpUp(): Promise<void> {
  console.log('\nTesting repeated bump up operations...');
  
  // Insert 20 users sequentially
  for (let i = 0; i < 20; i++) {
    await waitlist.insertUser(`bump_user${i}`, {
      email: `bump_user${i}@test.com`,
      metadata: { name: `Bump User ${i}` }
    });
  }

  // Move users to their final positions in reverse order
  console.log('\nMoving users to their final positions...');
  for (let i = 0; i < 20; i++) {
    // Move user[19-i] to position[i+1]
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
  console.log('\nTesting step-by-step bump up operations...');
  
  // Insert 20 users sequentially
  for (let i = 0; i < 20; i++) {
    await waitlist.insertUser(`bump_user${i}`, {
      email: `bump_user${i}@test.com`,
      metadata: { name: `Bump User ${i}` }
    });
  }

  // Move user19 up one position at a time
  console.log('\nMoving user19 up one position at a time...');
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
  console.log('\nTesting concurrent bump to same position...');
  
  // Insert 20 users sequentially
  for (let i = 0; i < 20; i++) {
    await waitlist.insertUser(`bump_user${i}`, {
      email: `bump_user${i}@test.com`,
      metadata: { name: `Bump User ${i}` }
    });
  }

  // Concurrently move users 16-19 to position 2
  console.log('\nMoving users 16,17,18,19 to position 2 concurrently...');
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
  console.log('\nTesting phone number attachment...');
  
  // Insert 20 users with emails only
  for (let i = 0; i < 20; i++) {
    await waitlist.insertUser(`user${i}`, {
      email: `user${i}@test.com`,
      metadata: { name: `User ${i}` }
    });
  }

  // Attach phone numbers to all users
  console.log('Attaching phone numbers...');
  const attachResults = await Promise.all(
    Array.from({ length: 20 }, (_, i) => 
      waitlist.attachPhone(`user${i}`, `+1555000${i.toString().padStart(4, '0')}`)
    )
  );

  // Verify all attachments succeeded
  if (!attachResults.every(result => result === true)) {
    throw new Error('Not all phone attachments succeeded');
  }

  // Verify phone mappings
  console.log('Verifying phone mappings...');
  for (let i = 0; i < 20; i++) {
    const userId = `user${i}`;
    const phone = `+1555000${i.toString().padStart(4, '0')}`;
    const mappedId = await waitlist._getPhoneMapping(phone);
    
    if (mappedId !== userId) {
      throw new Error(`Phone mapping incorrect for ${userId}. Expected ${userId}, got ${mappedId}`);
    }
  }

  // Try to attach an already used phone number
  console.log('Testing duplicate phone attachment...');
  const duplicateResult = await waitlist.attachPhone('user0', '+15550000001');
  if (duplicateResult !== false) {
    throw new Error('Should not be able to attach already used phone number');
  }

  // Verify original mapping wasn't affected
  const originalMapping = await waitlist._getPhoneMapping('+15550000001');
  if (originalMapping !== 'user1') {
    throw new Error(`Original phone mapping was affected. Expected user1, got ${originalMapping}`);
  }
}

async function testConcurrentDuplicateUserPhone(): Promise<void> {
  console.log('\nTesting concurrent insertion of same user by phone...');
  
  // First insert succeeds
  const phone = '+15550001234';
  const firstInsert = await waitlist.insertUser('user1', {
    phone,
    metadata: { name: 'User 1' }
  });

  // Try 5 concurrent inserts with same phone
  const attempts = 5;
  const results = await Promise.all(
    Array.from({ length: attempts }, (_, i) => 
      waitlist.insertUser(`user${i + 2}`, {
        phone,
        metadata: { name: `User ${i + 2}` }
      })
    )
  );

  // Verify results
  const onlyOneNewInsertion = results.every(r => r.already_existed);
  console.log('Only one new insertion:', onlyOneNewInsertion);

  const allSamePosition = results.every(r => r.position === firstInsert.position);
  console.log('All operations returned same position:', allSamePosition);
  console.log('Position returned:', firstInsert.position);

  const finalPosition = await waitlist.getPosition('user1');
  console.log('Final position matches:', finalPosition === firstInsert.position);

  if (!onlyOneNewInsertion || !allSamePosition || finalPosition !== firstInsert.position) {
    throw new Error('Concurrent phone insertion test failed');
  }

  // Verify only one user exists
  const finalOrder = await waitlist._getOrderedIds();
  if (finalOrder.length !== 1) {
    throw new Error(`Expected 1 user in waitlist, found ${finalOrder.length}`);
  }

  // Verify phone still maps to original user
  const mappedId = await waitlist._getPhoneMapping(phone);
  if (mappedId !== 'user1') {
    throw new Error(`Phone should map to user1, got ${mappedId}`);
  }
}

async function testEmailAttachment(): Promise<void> {
  console.log('\nTesting email attachment...');
  
  // Insert 20 users with phone numbers only
  for (let i = 0; i < 20; i++) {
    await waitlist.insertUser(`user${i}`, {
      phone: `+1555000${i.toString().padStart(4, '0')}`,
      metadata: { name: `User ${i}` }
    });
  }

  // Attach emails to all users
  console.log('Attaching emails...');
  const attachResults = await Promise.all(
    Array.from({ length: 20 }, (_, i) => 
      waitlist.attachEmail(`user${i}`, `user${i}@test.com`)
    )
  );

  // Verify all attachments succeeded
  if (!attachResults.every(result => result === true)) {
    throw new Error('Not all email attachments succeeded');
  }

  // Verify email mappings
  console.log('Verifying email mappings...');
  for (let i = 0; i < 20; i++) {
    const userId = `user${i}`;
    const email = `user${i}@test.com`;
    const mappedId = await waitlist._getEmailMapping(email);
    
    if (mappedId !== userId) {
      throw new Error(`Email mapping incorrect for ${userId}. Expected ${userId}, got ${mappedId}`);
    }
  }

  // Try to attach an already used email
  console.log('Testing duplicate email attachment...');
  const duplicateResult = await waitlist.attachEmail('user0', 'user1@test.com');
  if (duplicateResult !== false) {
    throw new Error('Should not be able to attach already used email');
  }

  // Verify original mapping wasn't affected
  const originalMapping = await waitlist._getEmailMapping('user1@test.com');
  if (originalMapping !== 'user1') {
    throw new Error(`Original email mapping was affected. Expected user1, got ${originalMapping}`);
  }
}

async function testLargeListBumpToTop(): Promise<void> {
  console.log('\nTesting bump to top in large list...');
  
  const totalUsers = 10000;
  
  // Insert users
  console.log(`Inserting ${totalUsers} users...`);
  const insertOperations = Array.from({ length: totalUsers }, (_, i) => {
    return waitlist.insertUser(`user${i}`, {
      email: `user${i}@test.com`,
      metadata: { name: `User ${i}` }
    });
  });

  await Promise.all(insertOperations);
  console.log('Users inserted');

  // Move last user to top
  const lastUserId = `user${totalUsers - 1}`;
  console.log(`Moving ${lastUserId} to position 1...`);
  await waitlist.bumpUserUp(lastUserId, 1);

  // Verify final positions
  console.log('Verifying positions...');
  
  // Check that last user is now first
  const topPosition = await waitlist.getPosition(lastUserId);
  if (topPosition !== 1) {
    throw new Error(`Expected ${lastUserId} at position 1, found at ${topPosition}`);
  }

  // Get final order and verify it's correct
  const finalOrder = await waitlist._getOrderedIds();
  
  // Verify length
  if (finalOrder.length !== totalUsers) {
    throw new Error(`Expected ${totalUsers} users, found ${finalOrder.length}`);
  }

  // Verify order: last user should be first, rest should be in original order
  const expectedOrder = [lastUserId].concat(
    Array.from({ length: totalUsers - 1 }, (_, i) => `user${i}`)
  );

  // Check entire list order
  for (let i = 0; i < totalUsers; i++) {
    if (finalOrder[i] !== expectedOrder[i]) {
      throw new Error(
        `Position ${i + 1} incorrect.\n` +
        `Expected: ${expectedOrder[i]}\n` +
        `Got: ${finalOrder[i]}`
      );
    }

    // Verify email mappings are still correct
    const userId = finalOrder[i];
    const email = `${userId}@test.com`;
    const mappedId = await waitlist._getEmailMapping(email);
    if (mappedId !== userId) {
      throw new Error(`Email mapping incorrect for ${userId}. Expected ${userId}, got ${mappedId}`);
    }
  }

  console.log('All positions and mappings verified');
}

async function runAllTests(): Promise<void> {
 let success = false;
 
 try {
   console.log('Starting waitlist tests...');
   
   await setup();
   await testConcurrentEmailUsers();

   await setup();
   await testPhoneAttachment();
   
   await setup();
   await testEmailAttachment();

   await setup();
   await testConcurrentDuplicateUserEmail();

   await setup();
   await testConcurrentDuplicateUserPhone();

   await setup();
   await testOrderedBumpUp();

   await setup();
   await testRepeatedBumpUp();

   await setup(); 
   await testLargeScaleBumpUp();

   await setup();
   await testStepByStepBumpUp();

   await setup();
   await testConcurrentBumpToSamePosition();
   
   await setup();
   await testLargeListBumpToTop();
   
   console.log('\nAll tests completed successfully!');
   success = true;
 } catch (error) {
   console.error('Test failed:', error);
   throw error;
 } finally {
   try {
     await setup();
     await waitlist.disconnect();
   } catch (error) {
     console.error('Final cleanup error:', error); 
   }
 }
}

process.on('unhandledRejection', async (error) => {
 console.error('Unhandled rejection:', error);
 await forceCleanup();
 process.exit(1);
});

runAllTests().catch(async () => {
 await forceCleanup();
 process.exit(1);
});