import { Redis } from 'ioredis';
import { InsertResult, RedisConfig, UserData } from './waitlistTypes';
import { v4 as uuidv4 } from 'uuid';

/**
 * WaitlistManager - A Redis-based waitlist management system
 * 
 * This class provides a comprehensive solution for managing a waitlist system with the following features:
 * - User registration with email and/or phone number
 * - Position tracking and management
 * - Invite code system with position bumping rewards
 * - Signup cutoff management
 * - User data storage and retrieval
 * 
 * The system uses Redis as its primary data store and implements atomic operations
 * using Redis Lua scripts to ensure data consistency.
 */
export class WaitlistManager {
  /** Redis client instance */
  protected redis: Redis;

  /** Maximum number of users allowed in the waitlist */
  protected lengthLimit: number = 100000;

  /** Maximum number of invite codes a user can create */
  protected inviteCodeLimit: number = 3;

  /** Length of generated invite codes */
  private inviteCodeLength: number = 6;

  /**
   * Position cutoff for signups. Users above this position cannot sign up.
   * -1 means no one can sign up (default)
   * 0 means everyone can sign up
   * n > 0 means only positions 1 through n can sign up
   */
  protected signupCutoff: number = -1;

  /**
   * Redis key mappings for various data structures
   * 
   * @property waitlist - Ordered list of user IDs (List)
   * @property users - User data storage (Hash: userId -> JSON userData)
   * @property emails - Email to userId mapping (Hash: email -> userId)
   * @property phones - Phone to userId mapping (Hash: phone -> userId)
   * @property inviteCodes - Invite code to creator mapping (Hash: code -> creatorId)
   * @property usedInviteCodes - Used invite codes mapping (Hash: code -> userId)
   * @property userInviteCodes - User's invite code count (Hash: userId -> count)
   * @property inviteCodeBumpPositions - Positions to bump for each code (Hash: code -> positions)
   * @property signedUp - Set of users who have signed up (Set)
   * @property codes - Hash: code -> JSON {maxUses, currentUses}
   */
  protected keys = {
    waitlist: 'waitlist:list',
    users: 'waitlist:users',
    emails: 'waitlist:emails',
    phones: 'waitlist:phones',
    inviteCodes: 'waitlist:invite_codes',
    usedInviteCodes: 'waitlist:used_codes',
    userInviteCodes: 'waitlist:user_codes',
    inviteCodeBumpPositions: 'waitlist:invite_code_bumps',
    signedUp: 'waitlist:signed_up',
    codes: 'waitlist:codes',
  };

  /**
   * Creates a new WaitlistManager instance
   * 
   * @param config - Redis configuration options
   * @throws Will throw an error if Redis connection fails
   */
  constructor(config: RedisConfig) {
    this.redis = new Redis(config);
  }

  /**
   * Sets the maximum number of users allowed in the waitlist
   * 
   * @param limit - Maximum number of users (must be non-negative)
   * @throws Error if limit is negative
   * 
   * @example
   * ```typescript
   * // Set waitlist limit to 5000 users
   * await waitlistManager.setListLimit(5000);
   * ```
   */
  setListLimit(limit: number): void {
    if (limit < 0) {
      throw new Error('Length limit cannot be negative');
    }
    this.lengthLimit = limit;
  }

  /**
   * Sets the maximum number of invite codes a user can create
   * 
   * @param limit - Maximum number of invite codes per user (must be non-negative)
   * @throws Error if limit is negative
   * 
   * @example
   * ```typescript
   * // Allow users to create up to 5 invite codes
   * await waitlistManager.setInviteCodeLimit(5);
   * ```
   */
  setInviteCodeLimit(limit: number): void {
    if (limit < 0) {
      throw new Error('Invite code limit cannot be negative');
    }
    this.inviteCodeLimit = limit;
  }

  /**
   * Sets the length of generated invite codes
   * 
   * @param length - Number of characters in generated invite codes (1-15)
   * @throws Error if length is less than 1 or greater than 15
   * 
   * @example
   * ```typescript
   * // Generate 8-character invite codes
   * await waitlistManager.setInviteCodeLength(8);
   * ```
   */
  setInviteCodeLength(length: number): void {
    if (length > 15) {
      throw new Error('Invite code length cannot exceed 15 characters');
    }
    if (length < 1) {
      throw new Error('Invite code length must be at least 1 character');
    }
    this.inviteCodeLength = length;
  }

  /**
   * Sets the position cutoff for user signups
   * 
   * @param cutoff - Position cutoff value:
   *   - -1: No one can sign up (closed waitlist)
   *   - 0: Everyone can sign up (open waitlist)
   *   - n > 0: Only positions 1 through n can sign up
   * @throws Error if cutoff is less than -1
   * 
   * If cutoff is larger than the current waitlist length, it will be
   * automatically adjusted to the waitlist length.
   * 
   * @example
   * ```typescript
   * // Allow only the first 100 users to sign up
   * await waitlistManager.setSignupCutoff(100);
   * 
   * // Close the waitlist completely
   * await waitlistManager.setSignupCutoff(-1);
   * 
   * // Open the waitlist to everyone
   * await waitlistManager.setSignupCutoff(0);
   * ```
   */
  async setSignupCutoff(cutoff: number): Promise<void> {
    if (cutoff < -1) {
      throw new Error('Signup cutoff cannot be less than -1');
    }
    // If cutoff is larger than list length, set it to list length
    const length = await this.redis.llen(this.keys.waitlist);
    this.signupCutoff = cutoff > length ? length : cutoff;
  }

  /**
   * Gracefully disconnects from Redis
   * Should be called when shutting down the application
   * 
   * @example
   * ```typescript
   * // When shutting down
   * await waitlistManager.disconnect();
   * ```
   */
  async disconnect(): Promise<void> {
    await this.redis.quit();
  }

  /**
   * Gets the current position of a user in the waitlist
   * 
   * @param id - User ID to look up
   * @returns Position in waitlist (1-based), or 0 if user not found
   * 
   * @example
   * ```typescript
   * const position = await waitlistManager.getPosition('user123');
   * if (position > 0) {
   *   console.log(`User is at position ${position}`);
   * } else {
   *   console.log('User not found in waitlist');
   * }
   * ```
   */
  async getPosition(id: string): Promise<number> {
    const script = `
      local id = ARGV[1]
      local items = redis.call('LRANGE', KEYS[1], 0, -1)
      for i, item in ipairs(items) do
        if item == id then
          return i
        end
      end
      return 0
    `;
    
    return await this.redis.eval(script, 1, this.keys.waitlist, id) as number;
  }

  /**
   * Inserts a new user into the waitlist
   * 
   * @param data - User data containing email and/or phone number
   * @returns Object containing:
   *   - id: The user's ID (new or existing)
   *   - position: User's position in waitlist (1-based)
   *   - already_existed: Whether user was already in waitlist
   * 
   * @throws Error if:
   *   - Neither email nor phone is provided
   *   - Waitlist is full (exceeds lengthLimit)
   * 
   * The method uses atomic operations to ensure:
   * 1. No duplicate emails/phones
   * 2. Consistent position assignment
   * 3. Proper data storage across all Redis structures
   * 
   * @example
   * ```typescript
   * const result = await waitlistManager.insertUser({
   *   email: 'user@example.com',
   *   phone: '+1234567890',
   *   metadata: { source: 'website' }
   * });
   * 
   * console.log(`User ${result.id} is at position ${result.position}`);
   * ```
   */
  async insertUser(data: UserData): Promise<InsertResult> {
    const id = uuidv4();
    if (!data.email && !data.phone) {
      throw new Error('Either email or phone must be provided');
    }

    const script = `
      local id = ARGV[1]
      local email = ARGV[2]
      local phone = ARGV[3]
      local limit = tonumber(ARGV[4])
      
      -- Check if user exists by email
      if email ~= '' then
        local existingIdByEmail = redis.call('HGET', KEYS[2], email)
        if existingIdByEmail then
          local pos = 0
          local items = redis.call('LRANGE', KEYS[1], 0, -1)
          for i, item in ipairs(items) do
            if item == existingIdByEmail then
              pos = i
              break
            end
          end
          return {pos, 1, existingIdByEmail}
        end
      end
      
      -- Check if user exists by phone
      if phone ~= '' then
        local existingIdByPhone = redis.call('HGET', KEYS[3], phone)
        if existingIdByPhone then
          local pos = 0
          local items = redis.call('LRANGE', KEYS[1], 0, -1)
          for i, item in ipairs(items) do
            if item == existingIdByPhone then
              pos = i
              break
            end
          end
          return {pos, 1, existingIdByPhone}
        end
      end
      
      -- Check if we're at the limit
      local currentLength = redis.call('LLEN', KEYS[1])
      if currentLength >= limit then
        return {-1, -1, ''}
      end
      
      -- Add to end of list
      redis.call('RPUSH', KEYS[1], id)
      
      if email ~= '' then
        redis.call('HSET', KEYS[2], email, id)
      end
      if phone ~= '' then
        redis.call('HSET', KEYS[3], phone, id)
      end
      
      return {currentLength + 1, 0, id}
    `;

    const [position, exists, existingId] = await this.redis.eval(
      script,
      3,
      this.keys.waitlist,
      this.keys.emails,
      this.keys.phones,
      id,
      data.email || '',
      data.phone || '',
      this.lengthLimit
    ) as [number, number, string];

    if (exists === -1) {
      throw new Error(`Waitlist is full (limit: ${this.lengthLimit})`);
    }

    const userId = exists === 1 ? existingId : id;
    await this.redis.hset(this.keys.users, userId, JSON.stringify({
      id: userId,
      email: data.email,
      phone: data.phone,
      metadata: data.metadata
    }));

    return {
      id: userId,
      position: position,
      already_existed: exists === 1
    };
  }

  /**
   * Attaches an email address to an existing user
   * 
   * @param id - User ID to attach email to
   * @param email - Email address to attach
   * @returns true if email was attached, false if user not found or email already in use
   * @throws Error if user has already signed up
   * 
   * This method is useful when:
   * - User initially registered with phone only
   * - User wants to add/change their email
   * 
   * @example
   * ```typescript
   * const success = await waitlistManager.attachEmail(
   *   'user123',
   *   'new@example.com'
   * );
   * if (success) {
   *   console.log('Email attached successfully');
   * }
   * ```
   */
  async attachEmail(id: string, email: string): Promise<boolean> {
    const isSignedUp = await this.isUserSignedUp(id);
    if (isSignedUp) {
      throw new Error('Cannot attach email to signed up user');
    }
    const script = `
      local id, email = ARGV[1], ARGV[2]
      
      -- Check if email is already used
      local existingId = redis.call('HGET', KEYS[1], email)
      if existingId then
        return 0
      end
      
      -- Check if user exists
      local userData = redis.call('HGET', KEYS[2], id)
      if not userData then
        return 0
      end
      
      redis.call('HSET', KEYS[1], email, id)
      return 1
    `;

    const result = await this.redis.eval(script, 2, this.keys.emails, this.keys.users, id, email) as number;
    
    if (result === 1) {
      const userData = JSON.parse(await this.redis.hget(this.keys.users, id) || '{}');
      userData.email = email;
      await this.redis.hset(this.keys.users, id, JSON.stringify(userData));
    }

    return result === 1;
  }

  /**
   * Attaches a phone number to an existing user
   * 
   * @param id - User ID to attach phone to
   * @param phone - Phone number to attach
   * @returns true if phone was attached, false if user not found or phone already in use
   * @throws Error if user has already signed up
   * 
   * This method is useful when:
   * - User initially registered with email only
   * - User wants to add/change their phone
   * 
   * @example
   * ```typescript
   * const success = await waitlistManager.attachPhone(
   *   'user123',
   *   '+1234567890'
   * );
   * if (success) {
   *   console.log('Phone attached successfully');
   * }
   * ```
   */
  async attachPhone(id: string, phone: string): Promise<boolean> {
    const isSignedUp = await this.isUserSignedUp(id);
    if (isSignedUp) {
      throw new Error('Cannot attach phone to signed up user');
    }
    const script = `
      local id, phone = ARGV[1], ARGV[2]
      
      -- Check if phone is already used
      local existingId = redis.call('HGET', KEYS[1], phone)
      if existingId then
        return 0
      end
      
      -- Check if user exists
      local userData = redis.call('HGET', KEYS[2], id)
      if not userData then
        return 0
      end
      
      redis.call('HSET', KEYS[1], phone, id)
      return 1
    `;

    const result = await this.redis.eval(script, 2, this.keys.phones, this.keys.users, id, phone) as number;
    
    if (result === 1) {
      const userData = JSON.parse(await this.redis.hget(this.keys.users, id) || '{}');
      userData.phone = phone;
      await this.redis.hset(this.keys.users, id, JSON.stringify(userData));
    }

    return result === 1;
  }

  /**
   * Moves a user to a new position in the waitlist
   * 
   * @param id - User ID to move
   * @param targetPosition - Desired position (1-based)
   * @returns true if move was successful, false otherwise
   * @throws Error if:
   *   - User has already signed up
   *   - User is within signup cutoff
   *   - Target position is at or above signup cutoff
   * 
   * The method uses a Redis lock to ensure atomic operations
   * and prevent race conditions during position changes.
   * 
   * Retries up to 3 times if lock acquisition fails.
   * 
   * @example
   * ```typescript
   * // Move user to position 100
   * const success = await waitlistManager.moveUser('user123', 100);
   * if (success) {
   *   console.log('User moved successfully');
   * }
   * ```
   */
  async moveUser(id: string, targetPosition: number): Promise<boolean> {
    const [isSignedUp, currentPos] = await Promise.all([
      this.isUserSignedUp(id),
      this.getPosition(id)
    ]);

    if (isSignedUp) {
      throw new Error('Cannot move user that has already signed up');
    }

    if (currentPos <= this.signupCutoff) {
      throw new Error('Cannot move user that is within signup cutoff');
    }

    if (targetPosition <= this.signupCutoff) {
      throw new Error('Cannot move user to position at or above signup cutoff');
    }

    const script = `
      local id, targetPos = ARGV[1], tonumber(ARGV[2])
      local lockKey = KEYS[1] .. ':lock'
      
      if redis.call('SET', lockKey, '1', 'NX', 'PX', 1000) == false then
        return 0
      end

      -- Check if list exists and has enough elements
      local listLen = tonumber(redis.call('LLEN', KEYS[1]))
      if listLen == 0 or targetPos < 1 or targetPos > listLen + 1 then
        redis.call('DEL', lockKey)
        return 0
      end

      -- Remove from current position (if exists)
      redis.call('LREM', KEYS[1], 1, tostring(id))
      
      if targetPos == 1 then
        -- Insert at head
        redis.call('LPUSH', KEYS[1], tostring(id))
      elseif targetPos > listLen then
        -- Insert at tail
        redis.call('RPUSH', KEYS[1], tostring(id))
      else
        -- Get pivot element and insert before it
        local pivot = redis.call('LINDEX', KEYS[1], targetPos - 1)
        if not pivot then
          redis.call('DEL', lockKey)
          return 0
        end
        redis.call('LINSERT', KEYS[1], 'BEFORE', tostring(pivot), tostring(id))
      end
      
      redis.call('DEL', lockKey)
      return 1
    `;

    let attempts = 3;
    while (attempts > 0) {
      const result = await this.redis.eval(script, 1, this.keys.waitlist, id, targetPosition);
      if (result === 1) return true;
      attempts--;
      if (attempts > 0) await new Promise(resolve => setTimeout(resolve, 100));
    }
    return false;
  }

  /**
   * Moves a user identified by email to a new position
   * 
   * @param email - Email address of user to move
   * @param targetPosition - Desired position (1-based)
   * @returns true if move was successful, false otherwise
   * @throws Error if user not found with email
   * 
   * Convenience method that wraps moveUser()
   * 
   * @example
   * ```typescript
   * await waitlistManager.moveUserByEmail(
   *   'user@example.com',
   *   100
   * );
   * ```
   */
  async moveUserByEmail(email: string, targetPosition: number): Promise<boolean> {
    const userId = await this.getEmailMapping(email);
    if (!userId) {
      throw new Error('User not found with this email');
    }
    return await this.moveUser(userId, targetPosition);
  }

  /**
   * Moves a user identified by phone to a new position
   * 
   * @param phone - Phone number of user to move
   * @param targetPosition - Desired position (1-based)
   * @returns true if move was successful, false otherwise
   * @throws Error if user not found with phone
   * 
   * Convenience method that wraps moveUser()
   * 
   * @example
   * ```typescript
   * await waitlistManager.moveUserByPhone(
   *   '+1234567890',
   *   100
   * );
   * ```
   */
  async moveUserByPhone(phone: string, targetPosition: number): Promise<boolean> {
    const userId = await this.getPhoneMapping(phone);
    if (!userId) {
      throw new Error('User not found with this phone');
    }
    return await this.moveUser(userId, targetPosition);
  }
 
  /**
   * Deletes a user from the waitlist
   * 
   * @param id - User ID to delete
   * @returns true if user was deleted, false if user not found
   * @throws Error if:
   *   - User has already signed up
   *   - User is within signup cutoff
   * 
   * This method atomically:
   * 1. Removes user from waitlist
   * 2. Deletes user data
   * 3. Removes email/phone mappings
   * 
   * @example
   * ```typescript
   * try {
   *   const deleted = await waitlistManager.deleteUser('user123');
   *   if (deleted) {
   *     console.log('User deleted successfully');
   *   }
   * } catch (error) {
   *   console.error('Cannot delete user:', error.message);
   * }
   * ```
   */
  async deleteUser(id: string): Promise<boolean> {
    const script = `
      local id = ARGV[1]
      local cutoff = tonumber(ARGV[2])
      
      -- Check if user exists
      local data = redis.call('HGET', KEYS[2], id)
      if not data then return 0 end
      
      -- Check if user is signed up
      if redis.call('SISMEMBER', KEYS[3], id) == 1 then
        return {err = 'Cannot delete user that has already signed up'}
      end
      
      -- Get user position
      local items = redis.call('LRANGE', KEYS[1], 0, -1)
      local position = 0
      for i, item in ipairs(items) do
        if item == id then
          position = i
          break
        end
      end
      
      -- Parse user data for email/phone
      local userData = cjson.decode(data)
      
      -- Delete user from all data structures
      redis.call('LREM', KEYS[1], 1, id)
      redis.call('HDEL', KEYS[2], id)
      if userData.email then
        redis.call('HDEL', KEYS[4], userData.email)
      end
      if userData.phone then
        redis.call('HDEL', KEYS[5], userData.phone)
      end
      
      return 1
    `;

    const result = await this.redis.eval(
      script,
      5,
      this.keys.waitlist,
      this.keys.users,
      this.keys.signedUp,
      this.keys.emails,
      this.keys.phones,
      id,
      this.signupCutoff
    ) as number | { err: string };

    if (typeof result === 'object' && 'err' in result) {
      throw new Error(result.err);
    }

    return result === 1;
  }

  /**
   * Deletes a user identified by email
   * 
   * @param email - Email address of user to delete
   * @returns true if user was deleted, false if user not found
   * @throws Same errors as deleteUser()
   * 
   * Convenience method that wraps deleteUser()
   * 
   * @example
   * ```typescript
   * const deleted = await waitlistManager.deleteUserByEmail('user@example.com');
   * ```
   */
  async deleteUserByEmail(email: string): Promise<boolean> {
    const userId = await this.getEmailMapping(email);
    if (!userId) return false;
    return await this.deleteUser(userId);
  }

  /**
   * Deletes a user identified by phone
   * 
   * @param phone - Phone number of user to delete
   * @returns true if user was deleted, false if user not found
   * @throws Same errors as deleteUser()
   * 
   * Convenience method that wraps deleteUser()
   * 
   * @example
   * ```typescript
   * const deleted = await waitlistManager.deleteUserByPhone('+1234567890');
   * ```
   */
  async deleteUserByPhone(phone: string): Promise<boolean> {
    const userId = await this.getPhoneMapping(phone);
    if (!userId) return false;
    return await this.deleteUser(userId);
  }

  /**
   * Gets all user IDs in waitlist order
   * 
   * @returns Array of user IDs in waitlist order (1-based indexing)
   * 
   * Warning: This method returns the entire waitlist. For large lists,
   * consider implementing pagination or using getPosition() instead.
   * 
   * @example
   * ```typescript
   * const ids = await waitlistManager.getOrderedIds();
   * console.log(`Total users: ${ids.length}`);
   * ```
   */
  async getOrderedIds(): Promise<string[]> {
    return await this.redis.lrange(this.keys.waitlist, 0, -1);
  }

  /**
   * Gets the user ID associated with an email address
   * 
   * @param email - Email address to look up
   * @returns User ID if found, null otherwise
   * 
   * @example
   * ```typescript
   * const userId = await waitlistManager.getEmailMapping('user@example.com');
   * if (userId) {
   *   console.log(`Found user: ${userId}`);
   * }
   * ```
   */
  async getEmailMapping(email: string): Promise<string | null> {
    return await this.redis.hget(this.keys.emails, email);
  }

  /**
   * Gets the user ID associated with a phone number
   * 
   * @param phone - Phone number to look up
   * @returns User ID if found, null otherwise
   * 
   * @example
   * ```typescript
   * const userId = await waitlistManager.getPhoneMapping('+1234567890');
   * if (userId) {
   *   console.log(`Found user: ${userId}`);
   * }
   * ```
   */
  async getPhoneMapping(phone: string): Promise<string | null> {
    return await this.redis.hget(this.keys.phones, phone);
  }

  /**
   * Gets the current length of the waitlist
   * 
   * @returns Number of users currently in the waitlist
   * 
   * @example
   * ```typescript
   * const length = await waitlistManager.getLength();
   * console.log(`Waitlist has ${length} users`);
   * ```
   */
  async getLength(): Promise<number> {
    return await this.redis.llen(this.keys.waitlist);
  }

  /**
   * Creates a new invite code for a user
   * 
   * @param userId - ID of user creating the invite code
   * @param minBumpPositions - Minimum positions to bump when code is used
   * @returns Generated invite code
   * @throws Error if:
   *   - User has already signed up
   *   - User has reached their invite code limit
   *   - Failed to generate unique code after 50 attempts
   * 
   * The generated code:
   * - Is alphanumeric (A-Z, 0-9)
   * - Has length specified by inviteCodeLength
   * - Is guaranteed unique
   * 
   * @example
   * ```typescript
   * try {
   *   // Create code that bumps at least 10 positions
   *   const code = await waitlistManager.createInviteCode('user123', 10);
   *   console.log(`Generated code: ${code}`);
   * } catch (error) {
   *   console.error('Failed to create code:', error.message);
   * }
   * ```
   */
  async createInviteCode(userId: string, minBumpPositions: number): Promise<string> {
    const isSignedUp = await this.isUserSignedUp(userId);
    if (isSignedUp) {
      throw new Error('Signed up users cannot create invite codes');
    }
    const script = `
      local userId, minBumpPositions, codeLength, limit = ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4])
      
      -- Check if user has reached their limit
      local userCodeCount = tonumber(redis.call('HGET', KEYS[3], userId) or '0')
      if userCodeCount >= limit then
        return {err = 'Invite code limit reached'}
      end
      
      -- Generate and try codes until we find an unused one
      local attempts = 0
      local maxAttempts = 50
      local chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
      local charsLength = string.len(chars)
      
      while attempts < maxAttempts do
        -- Generate a random code of specified length
        local code = ""
        for i = 1, codeLength do
          local randIndex = math.random(1, charsLength)
          code = code .. string.sub(chars, randIndex, randIndex)
        end
        
        -- Check if code exists
        if redis.call('HEXISTS', KEYS[1], code) == 0 then
          -- Store the code
          redis.call('HSET', KEYS[1], code, userId)
          -- Store minimum bump positions
          redis.call('HSET', KEYS[4], code, minBumpPositions)
          -- Add to user's set of codes
          redis.call('SADD', KEYS[3] .. ':' .. userId, code)
          -- Increment user's code count
          redis.call('HINCRBY', KEYS[3], userId, 1)
          return code  -- Return the code directly, not in a table
        end
        attempts = attempts + 1
      end
      
      return {err = 'Failed to generate unique code'}
    `;

    const result = await this.redis.eval(
      script,
      4,
      this.keys.inviteCodes,
      this.keys.users,
      this.keys.userInviteCodes,
      this.keys.inviteCodeBumpPositions,
      userId,
      minBumpPositions,
      this.inviteCodeLength,  // Pass the code length to Lua
      this.inviteCodeLimit
    );

    if (typeof result === 'string') {
      return result;
    }

    if (result && typeof result === 'object' && 'err' in result) {
      throw new Error((result as { err: string }).err);
    }

    throw new Error('Unexpected response from Redis');
  }

  /**
   * Uses an invite code to join the waitlist
   * 
   * @param code - Invite code to use
   * @param userData - New user's data (email/phone)
   * @param bumpPositions - Requested positions to bump code creator
   * @returns Object containing new user's ID and position
   * @throws Error if:
   *   - Invalid/used invite code
   *   - Code creator no longer in waitlist
   *   - Code creator has signed up
   *   - Waitlist is full
   *   - Email/phone already exists
   * 
   * The method:
   * 1. Validates the code and creator status
   * 2. Creates new user entry
   * 3. Moves creator up by max(requested, minimum) positions
   * 4. Marks code as used
   * 
   * @example
   * ```typescript
   * try {
   *   const result = await waitlistManager.useInviteCode(
   *     'ABC123',
   *     { email: 'new@example.com' },
   *     15  // Request 15 position bump
   *   );
   *   console.log(`New user ${result.id} at position ${result.position}`);
   * } catch (error) {
   *   console.error('Failed to use code:', error.message);
   * }
   * ```
   */
  async useInviteCode(code: string, userData: UserData, bumpPositions: number): Promise<InsertResult> {
    const creatorId = await this.getInviteCodeCreator(code);
    if (!creatorId) {
      throw new Error('Invalid invite code');
    }

    // Check if creator is still in the waitlist
    const creatorPosition = await this.getPosition(creatorId);
    if (creatorPosition === 0) {
      throw new Error('Invite code creator no longer in waitlist');
    }

    const isCreatorSignedUp = await this.isUserSignedUp(creatorId);
    if (isCreatorSignedUp) {
      throw new Error('Cannot use invite code from signed up user');
    }

    const id = uuidv4();
    if (!userData.email && !userData.phone) {
      throw new Error('Either email or phone must be provided');
    }

    const script = `
      local code, id, email, phone = ARGV[1], ARGV[2], ARGV[3], ARGV[4]
      local requestedBumpPositions = tonumber(ARGV[5])
      local limit = tonumber(ARGV[6])
      
      -- Check if code exists and get creator
      local creatorId = redis.call('HGET', KEYS[3], code)
      if not creatorId then return {err = 'Invalid invite code'} end
      
      -- Check if we're at the limit
      local currentLength = redis.call('LLEN', KEYS[1])
      if currentLength >= limit then
        return {err = 'Waitlist is full'} 
      end
      
      -- Get minimum bump positions for this code
      local minBumpPositions = tonumber(redis.call('HGET', KEYS[7], code) or '0')
      local finalBumpPositions = math.max(requestedBumpPositions, minBumpPositions)
      
      -- Check if code is already used
      if redis.call('HEXISTS', KEYS[4], code) == 1 then
        return {err = 'Invite code already used'}
      end
      
      -- Check if email exists
      if email ~= '' then
        local existingId = redis.call('HGET', KEYS[5], email)
        if existingId then return {err = 'Email already exists'} end
      end
      
      -- Check if phone exists
      if phone ~= '' then
        local existingId = redis.call('HGET', KEYS[6], phone)
        if existingId then return {err = 'Phone already exists'} end
      end
      
      -- Mark code as used
      redis.call('HSET', KEYS[4], code, id)
      
      -- Find creator's current position
      local items = redis.call('LRANGE', KEYS[1], 0, -1)
      local creatorPos = 0
      for i, item in ipairs(items) do
        if item == creatorId then
          creatorPos = i
          break
        end
      end
      if creatorPos == 0 then return {err = 'Creator not found in waitlist'} end
      
      -- Calculate target position for creator (cannot go above 1)
      local targetPos = math.max(1, creatorPos - finalBumpPositions)
      
      -- Add new user at the end first
      redis.call('RPUSH', KEYS[1], id)
      
      -- Store email/phone mappings
      if email ~= '' then
        redis.call('HSET', KEYS[5], email, id)
      end
      if phone ~= '' then
        redis.call('HSET', KEYS[6], phone, id)
      end
      
      -- Move creator to new position
      redis.call('LREM', KEYS[1], 1, creatorId)
      if targetPos == 1 then
        redis.call('LPUSH', KEYS[1], creatorId)
      else
        local pivot = redis.call('LINDEX', KEYS[1], targetPos - 1)
        redis.call('LINSERT', KEYS[1], 'BEFORE', pivot, creatorId)
      end
      
      redis.call('HSET', KEYS[4], code, id)  -- Store which user used this code
      redis.call('HSET', KEYS[4] .. ':reverse', id, code)  -- Add reverse mapping for lookup
      
      return {ok = id}
    `;

    const result = await this.redis.eval(
      script,
      7,
      this.keys.waitlist,
      this.keys.users,
      this.keys.inviteCodes,
      this.keys.usedInviteCodes,
      this.keys.emails,
      this.keys.phones,
      this.keys.inviteCodeBumpPositions,
      code,
      id,
      userData.email || '',
      userData.phone || '',
      bumpPositions,
      this.lengthLimit
    ) as { err?: string; ok?: string };

    if (result.err) {
      throw new Error(result.err);
    }

    // Store full user data
    await this.redis.hset(this.keys.users, id, JSON.stringify({
      id,
      email: userData.email,
      phone: userData.phone,
      metadata: userData.metadata
    }));

    return {
      id,
      position: await this.getPosition(id),
      already_existed: false
    };
  }

  /**
   * Gets the user ID of the invite code creator
   * 
   * @param code - Invite code to look up
   * @returns Creator's user ID if found, null if code doesn't exist
   * 
   * @example
   * ```typescript
   * const creatorId = await waitlistManager.getInviteCodeCreator('ABC123');
   * if (creatorId) {
   *   console.log(`Code created by user: ${creatorId}`);
   * }
   * ```
   */
  async getInviteCodeCreator(code: string): Promise<string | null> {
    return await this.redis.hget(this.keys.inviteCodes, code);
  }

  /**
   * Gets the user ID of who used an invite code
   * 
   * @param code - Invite code to look up
   * @returns User ID of person who used the code, null if unused
   * 
   * @example
   * ```typescript
   * const userId = await waitlistManager.getInviteCodeUser('ABC123');
   * if (userId) {
   *   console.log(`Code was used by: ${userId}`);
   * }
   * ```
   */
  async getInviteCodeUser(code: string): Promise<string | null> {
    return await this.redis.hget(this.keys.usedInviteCodes, code);
  }

  /**
   * Gets the number of invite codes created by a user
   * 
   * @param userId - User ID to check
   * @returns Number of codes created by user
   * 
   * @example
   * ```typescript
   * const count = await waitlistManager.getUserInviteCodeCount('user123');
   * console.log(`User has created ${count} codes`);
   * ```
   */
  async getUserInviteCodeCount(userId: string): Promise<number> {
    const count = await this.redis.hget(this.keys.userInviteCodes, userId);
    return count ? parseInt(count) : 0;
  }

  /**
   * Calculates the position a code creator would move to if their code is used
   * 
   * @param code - Invite code to check
   * @returns Target position after bump, null if code invalid/used
   * 
   * This is useful for showing potential users what position
   * they could achieve by using their code.
   * 
   * @example
   * ```typescript
   * const position = await waitlistManager.getPositionAfterInviteCodeUse('ABC123');
   * if (position) {
   *   console.log(`Using this code will move you to position ${position}`);
   * }
   * ```
   */
  async getPositionAfterInviteCodeUse(code: string): Promise<number | null> {
    const script = `
      local code = ARGV[1]
      
      -- Check if code exists and get creator
      local creatorId = redis.call('HGET', KEYS[1], code)
      if not creatorId then return nil end
      
      -- Check if code is already used
      if redis.call('HEXISTS', KEYS[2], code) == 1 then return nil end
      
      -- Get creator's current position
      local items = redis.call('LRANGE', KEYS[4], 0, -1)
      local creatorPos = 0
      for i, item in ipairs(items) do
        if item == creatorId then
          creatorPos = i
          break
        end
      end
      if creatorPos == 0 then return nil end
      
      -- Get minimum bump positions for this code
      local minBumpPositions = tonumber(redis.call('HGET', KEYS[3], code) or '0')
      
      -- Calculate target position (cannot go above 1)
      return math.max(1, creatorPos - minBumpPositions)
    `;

    const result = await this.redis.eval(
      script,
      4,
      this.keys.inviteCodes,
      this.keys.usedInviteCodes,
      this.keys.inviteCodeBumpPositions,
      this.keys.waitlist,
      code
    ) as number | null;

    return result;
  }

  /**
   * Calculates potential position after using an invite code, looking up user by email
   * 
   * @param email - Email address of user to check
   * @param code - Invite code to evaluate
   * @returns Target position after bump, null if code invalid/used
   * @throws Error if user not found with email
   * 
   * Convenience method that wraps getPositionAfterInviteCodeUse()
   * 
   * @example
   * ```typescript
   * const position = await waitlistManager.getPositionAfterInviteCodeUseByEmail(
   *   'user@example.com',
   *   'ABC123'
   * );
   * ```
   */
  async getPositionAfterInviteCodeUseByEmail(email: string, code: string): Promise<number | null> {
    const userId = await this.getEmailMapping(email);
    if (!userId) {
      throw new Error('User not found with this email');
    }
    return await this.getPositionAfterInviteCodeUse(code);
  }

  /**
   * Calculates potential position after using an invite code, looking up user by phone
   * 
   * @param phone - Phone number of user to check
   * @param code - Invite code to evaluate
   * @returns Target position after bump, null if code invalid/used
   * @throws Error if user not found with phone
   * 
   * Convenience method that wraps getPositionAfterInviteCodeUse()
   * 
   * @example
   * ```typescript
   * const position = await waitlistManager.getPositionAfterInviteCodeUseByPhone(
   *   '+1234567890',
   *   'ABC123'
   * );
   * ```
   */
  async getPositionAfterInviteCodeUseByPhone(phone: string, code: string): Promise<number | null> {
    const userId = await this.getPhoneMapping(phone);
    if (!userId) {
      throw new Error('User not found with this phone');
    }
    return await this.getPositionAfterInviteCodeUse(code);
  }

  /**
   * Gets the minimum number of positions a code will bump its creator
   * 
   * @param code - Invite code to check
   * @returns Minimum bump positions for the code, 0 if code doesn't exist
   * 
   * @example
   * ```typescript
   * const positions = await waitlistManager.getInviteCodeBumpPositions('ABC123');
   * console.log(`This code bumps at least ${positions} positions`);
   * ```
   */
  async getInviteCodeBumpPositions(code: string): Promise<number> {
    const positions = await this.redis.hget(this.keys.inviteCodeBumpPositions, code);
    return positions ? parseInt(positions) : 0;
  }

  /**
   * Gets all invite codes created by a user
   * 
   * @param userId - User ID to check
   * @returns Array of invite codes created by the user
   * 
   * @example
   * ```typescript
   * const codes = await waitlistManager.getUserCreatedInviteCodes('user123');
   * console.log(`User has created these codes: ${codes.join(', ')}`);
   * ```
   */
  async getUserCreatedInviteCodes(userId: string): Promise<string[]> {
    const script = `
      local userId = ARGV[1]
      return redis.call('SMEMBERS', KEYS[1] .. ':' .. userId)
    `;

    return await this.redis.eval(
      script,
      1,
      this.keys.userInviteCodes,
      userId
    ) as string[];
  }

  /**
   * Gets information about the invite code used by a user
   * 
   * @param userId - User ID to check
   * @returns Object containing:
   *   - code: The invite code used (null if none used)
   *   - creatorId: ID of code creator (null if no code used)
   *   - creatorEmail: Email of code creator (null if no email or no code used)
   *   - creatorPhone: Phone of code creator (null if no phone or no code used)
   * 
   * @example
   * ```typescript
   * const info = await waitlistManager.getInviteCodeUsedBy('user123');
   * if (info.code) {
   *   console.log(`User used code ${info.code} created by ${info.creatorEmail}`);
   * }
   * ```
   */
  async getInviteCodeUsedBy(userId: string): Promise<{ 
    code: string | null, 
    creatorId: string | null,
    creatorEmail: string | null,
    creatorPhone: string | null 
  }> {
    const script = `
      local userId = ARGV[1]
      
      -- Get the invite code used by this user from reverse mapping
      local code = redis.call('HGET', KEYS[1] .. ':reverse', userId)
      if not code then
        return {false, false, false, false}
      end
      
      -- Get the creator of this code
      local creatorId = redis.call('HGET', KEYS[2], code)
      if not creatorId then
        return {code, false, false, false}
      end
      
      -- Get creator's data
      local creatorData = redis.call('HGET', KEYS[3], creatorId)
      if not creatorData then
        return {code, creatorId, false, false}
      end
      
      local creator = cjson.decode(creatorData)
      return {code, creatorId, creator.email or false, creator.phone or false}
    `;

    const [code, creatorId, creatorEmail, creatorPhone] = await this.redis.eval(
      script,
      3,
      this.keys.usedInviteCodes,
      this.keys.inviteCodes,
      this.keys.users,
      userId
    ) as [string | false, string | false, string | false, string | false];

    return {
      code: code || null,
      creatorId: creatorId || null,
      creatorEmail: creatorEmail || null,
      creatorPhone: creatorPhone || null
    };
  }

  /**
   * Checks if a user has signed up
   * 
   * @param id - User ID to check
   * @returns true if user has signed up, false otherwise
   * 
   * @example
   * ```typescript
   * if (await waitlistManager.isUserSignedUp('user123')) {
   *   console.log('User has already signed up');
   * }
   * ```
   */
  async isUserSignedUp(id: string): Promise<boolean> {
    return (await this.redis.sismember(this.keys.signedUp, id)) === 1;
  }

  /**
   * Checks if a user is eligible to sign up
   * 
   * @param id - User ID to check
   * @returns true if user can sign up, false otherwise
   * 
   * A user can sign up if:
   * 1. They exist in the waitlist
   * 2. Their position is within the signup cutoff
   * 3. They haven't already signed up
   * 
   * @example
   * ```typescript
   * if (await waitlistManager.canUserSignUp('user123')) {
   *   console.log('User can proceed with signup');
   * }
   * ```
   */
  async canUserSignUp(id: string): Promise<boolean> {
    const [position, isSignedUp] = await Promise.all([
      this.getPosition(id),
      this.isUserSignedUp(id)
    ]);
    return position > 0 && position <= this.signupCutoff && !isSignedUp;
  }

  /**
   * Marks a user as signed up
   * 
   * @param id - User ID to mark as signed up
   * @returns true if user was marked as signed up, false if:
   *   - User doesn't exist
   *   - User is outside signup cutoff
   *   - User is already signed up
   * 
   * This method is atomic and ensures that only eligible users
   * can be marked as signed up.
   * 
   * @example
   * ```typescript
   * if (await waitlistManager.markUserAsSignedUp('user123')) {
   *   console.log('User successfully marked as signed up');
   * } else {
   *   console.log('User not eligible for signup');
   * }
   * ```
   */
  async markUserAsSignedUp(id: string): Promise<boolean> {
    const script = `
      local id = ARGV[1]
      local cutoff = tonumber(ARGV[2])
      
      -- Check if user exists and get position
      local items = redis.call('LRANGE', KEYS[1], 0, -1)
      local position = 0
      for i, item in ipairs(items) do
        if item == id then
          position = i
          break
        end
      end
      
      -- Verify user can sign up
      if position == 0 or position > cutoff then
        return 0
      end
      
      -- Check if already signed up
      if redis.call('SISMEMBER', KEYS[2], id) == 1 then
        return 0
      end
      
      -- Mark as signed up
      redis.call('SADD', KEYS[2], id)
      return 1
    `;

    const result = await this.redis.eval(
      script,
      2,
      this.keys.waitlist,
      this.keys.signedUp,
      id,
      this.signupCutoff
    ) as number;

    return result === 1;
  }

  /**
   * Creates a new code with a specified usage limit
   * 
   * @param code - The code to create
   * @param maxUses - Maximum number of times this code can be used
   * @returns true if code was created, false if code already exists
   * 
   * @example
   * ```typescript
   * const created = await waitlistManager.createCommunityCode('LAUNCH2024', 100);
   * if (created) {
   *   console.log('Code created successfully');
   * }
   * ```
   */
  async createCommunityCode(code: string, maxUses: number): Promise<boolean> {
    if (maxUses < 1) {
      throw new Error('Maximum uses must be at least 1');
    }

    const exists = await this.redis.hexists(this.keys.codes, code);
    if (exists) {
      return false;
    }

    await this.redis.hset(this.keys.codes, code, JSON.stringify({
      maxUses,
      currentUses: 0
    }));

    return true;
  }

  /**
   * Uses a code to sign up a user immediately
   * 
   * @param code - Code to use
   * @param userData - User data containing email and/or phone
   * @returns true if successful, error message if unsuccessful
   * 
   * @example
   * ```typescript
   * const result = await waitlistManager.useCommunityCode('LAUNCH2024', {
   *   email: 'user@example.com',
   *   phone: '+1234567890'
   * });
   * if (result === true) {
   *   console.log('Code used successfully');
   * } else {
   *   console.log('Failed:', result);
   * }
   * ```
   */
  async useCommunityCode(code: string, userData: UserData): Promise<true | string> {
    if (!userData.email && !userData.phone) {
      return 'Either email or phone must be provided';
    }

    const script = `
      local code = ARGV[1]
      local email = ARGV[2]
      local phone = ARGV[3]
      
      -- Get code data and increment atomically with HINCRBY
      local codeData = redis.call('HGET', KEYS[1], code)
      if not codeData then
        return {err = 'Invalid code'}
      end
      
      local codeInfo = cjson.decode(codeData)
      -- Use HINCRBY to atomically increment and check
      local newUses = redis.call('HINCRBY', KEYS[1] .. ':uses', code, 1)
      
      if newUses > codeInfo.maxUses then
        -- Rollback the increment if we went over
        redis.call('HINCRBY', KEYS[1] .. ':uses', code, -1)
        return {err = 'Code has reached usage limit'}
      end

      -- Rest of the checks...
      local usageKey = 'waitlist:code:' .. code .. ':users'
      if email ~= '' then
        if redis.call('SISMEMBER', usageKey, email) == 1 then
          redis.call('HINCRBY', KEYS[1] .. ':uses', code, -1)
          return {err = 'Code already used by this email'}
        end
      end
      if phone ~= '' then
        if redis.call('SISMEMBER', usageKey, phone) == 1 then
          redis.call('HINCRBY', KEYS[1] .. ':uses', code, -1)
          return {err = 'Code already used by this phone'}
        end
      end
      
      -- Update the code info
      codeInfo.currentUses = newUses
      redis.call('HSET', KEYS[1], code, cjson.encode(codeInfo))
      
      -- Track usage
      if email ~= '' then
        redis.call('SADD', usageKey, email)
      end
      if phone ~= '' then
        redis.call('SADD', usageKey, phone)
      end

      -- If user exists in waitlist, mark them as signed up
      local existingId = nil
      if email ~= '' then
        existingId = redis.call('HGET', KEYS[2], email)
      end
      if not existingId and phone ~= '' then
        existingId = redis.call('HGET', KEYS[3], phone)
      end
      
      if existingId then
        redis.call('SADD', KEYS[4], existingId)
      end
      
      return {ok = true}
    `;

    const result = await this.redis.eval(
      script,
      4,
      this.keys.codes,
      this.keys.emails,
      this.keys.phones,
      this.keys.signedUp,
      code,
      userData.email || '',
      userData.phone || ''
    ) as { err?: string; ok?: boolean };

    if (result.err) {
      return result.err;
    }

    return true;
  }

  /**
   * Gets information about a code's usage
   * 
   * @param code - Code to check
   * @returns Object containing usage information, or null if code doesn't exist
   * 
   * @example
   * ```typescript
   * const info = await waitlistManager.getCommunityCodeInfo('LAUNCH2024');
   * if (info) {
   *   console.log(`Code used ${info.currentUses}/${info.maxUses} times`);
   * }
   * ```
   */
  async getCommunityCodeInfo(code: string): Promise<{ 
    maxUses: number;
    currentUses: number;
    remainingUses: number;
  } | null> {
    const [data, uses] = await Promise.all([
      this.redis.hget(this.keys.codes, code),
      this.redis.hget(this.keys.codes + ':uses', code)
    ]);
    
    if (!data) {
      return null;
    }

    const info = JSON.parse(data);
    const currentUses = uses ? parseInt(uses) : 0;
    
    return {
      maxUses: info.maxUses,
      currentUses,
      remainingUses: info.maxUses - currentUses
    };
  }

  /**
   * Deletes a community code and all its associated data
   * 
   * @param code - Code to delete
   * @returns true if code was deleted, false if code didn't exist
   * 
   * @example
   * ```typescript
   * const deleted = await waitlistManager.deleteCommunityCode('LAUNCH2024');
   * if (deleted) {
   *   console.log('Code deleted successfully');
   * }
   * ```
   */
  async deleteCommunityCode(code: string): Promise<boolean> {
    const script = `
      local code = ARGV[1]
      
      -- Atomically delete and return if anything was deleted
      local deleted = redis.call('HDEL', KEYS[1], code)
      if deleted == 0 then
        return false
      end
      
      -- Code existed and was deleted, now clean up related data
      redis.call('HDEL', KEYS[1] .. ':uses', code)
      redis.call('DEL', 'waitlist:code:' .. code .. ':users')
      
      return true
    `;

    const result = await this.redis.eval(
      script,
      1,
      this.keys.codes,
      code
    ) as number;  // Redis returns 1/0 for true/false

    return result === 1;  // Convert to boolean
  }
}