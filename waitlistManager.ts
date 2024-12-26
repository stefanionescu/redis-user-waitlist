import { Redis } from 'ioredis';
import { InsertResult, RedisConfig, UserData } from './waitlistTypes';

export class WaitlistManager {
  protected redis: Redis;
  protected lengthLimit: number = 100000;
  protected inviteCodeLimit: number = 3;
  protected keys = {
    waitlist: 'waitlist:list',
    users: 'waitlist:users',
    emails: 'waitlist:emails',
    phones: 'waitlist:phones',
    inviteCodes: 'waitlist:invite_codes',
    usedInviteCodes: 'waitlist:used_codes',
    userInviteCodes: 'waitlist:user_codes',
    inviteCodeBumpPositions: 'waitlist:invite_code_bumps'
  };
 
  constructor(config: RedisConfig) {
    this.redis = new Redis(config);
  }
 
  setLengthLimit(limit: number): void {
    if (limit < 0) {
      throw new Error('Length limit cannot be negative');
    }
    this.lengthLimit = limit;
  }
 
  async insertUser(id: string, data: UserData): Promise<InsertResult> {
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

    return {position: position, already_existed: exists === 1};
  }

  async attachEmail(id: string, email: string): Promise<boolean> {
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

  async attachPhone(id: string, phone: string): Promise<boolean> {
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

  async moveUser(id: string, targetPosition: number): Promise<boolean> {
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

  async moveUserByEmail(email: string, targetPosition: number): Promise<boolean> {
    const userId = await this.getEmailMapping(email);
    if (!userId) {
      throw new Error('User not found with this email');
    }
    return await this.moveUser(userId, targetPosition);
  }

  async moveUserByPhone(phone: string, targetPosition: number): Promise<boolean> {
    const userId = await this.getPhoneMapping(phone);
    if (!userId) {
      throw new Error('User not found with this phone');
    }
    return await this.moveUser(userId, targetPosition);
  }
 
  async deleteUser(id: string): Promise<boolean> {
    const data = await this.redis.hget(this.keys.users, id);
    if (!data) return false;
    
    const {email, phone} = JSON.parse(data);
    await this.redis.multi()
      .lrem(this.keys.waitlist, 1, id)
      .hdel(this.keys.users, id)
      .hdel(this.keys.emails, email)
      .hdel(this.keys.phones, phone)
      .exec();
    return true;
  }

  async deleteUserByEmail(email: string): Promise<boolean> {
    const script = `
      local email = ARGV[1]
      
      -- Get user ID from email
      local userId = redis.call('HGET', KEYS[3], email)
      if not userId then return 0 end
      
      -- Get full user data
      local userData = redis.call('HGET', KEYS[2], userId)
      if not userData then return 0 end
      
      -- Parse user data to get phone
      local data = cjson.decode(userData)
      local phone = data.phone or ''
      
      -- Remove from all data structures
      redis.call('LREM', KEYS[1], 1, userId)
      redis.call('HDEL', KEYS[2], userId)
      redis.call('HDEL', KEYS[3], email)
      if phone ~= '' then
        redis.call('HDEL', KEYS[4], phone)
      end
      
      return 1
    `;

    const result = await this.redis.eval(
      script,
      4,
      this.keys.waitlist,
      this.keys.users,
      this.keys.emails,
      this.keys.phones,
      email
    ) as number;

    return result === 1;
  }

  async deleteUserByPhone(phone: string): Promise<boolean> {
    const script = `
      local phone = ARGV[1]
      
      -- Get user ID from phone
      local userId = redis.call('HGET', KEYS[4], phone)
      if not userId then return 0 end
      
      -- Get full user data
      local userData = redis.call('HGET', KEYS[2], userId)
      if not userData then return 0 end
      
      -- Parse user data to get email
      local data = cjson.decode(userData)
      local email = data.email or ''
      
      -- Remove from all data structures
      redis.call('LREM', KEYS[1], 1, userId)
      redis.call('HDEL', KEYS[2], userId)
      if email ~= '' then
        redis.call('HDEL', KEYS[3], email)
      end
      redis.call('HDEL', KEYS[4], phone)
      
      return 1
    `;

    const result = await this.redis.eval(
      script,
      4,
      this.keys.waitlist,
      this.keys.users,
      this.keys.emails,
      this.keys.phones,
      phone
    ) as number;

    return result === 1;
  }
 
  async disconnect(): Promise<void> {
    await this.redis.quit();
  }

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
 
  async getOrderedIds(): Promise<string[]> {
    return await this.redis.lrange(this.keys.waitlist, 0, -1);
  }
 
  async getEmailMapping(email: string): Promise<string | null> {
    return await this.redis.hget(this.keys.emails, email);
  }

  async getPhoneMapping(phone: string): Promise<string | null> {
    return await this.redis.hget(this.keys.phones, phone);
  }

  async getLength(): Promise<number> {
    return await this.redis.llen(this.keys.waitlist);
  }

  setInviteCodeLimit(limit: number): void {
    if (limit < 0) {
      throw new Error('Invite code limit cannot be negative');
    }
    this.inviteCodeLimit = limit;
  }

  async createInviteCode(userId: string, minBumpPositions: number): Promise<string> {
    if (minBumpPositions < 0) {
      throw new Error('Minimum bump positions cannot be negative');
    }

    const script = `
      local userId = ARGV[1]
      local limit = tonumber(ARGV[2])
      local minBumpPositions = tonumber(ARGV[3])
      
      -- Check if user exists
      local userData = redis.call('HGET', KEYS[2], userId)
      if not userData then return {err = 'User not found'} end
      
      -- Check if user is at their invite code limit
      local currentCodes = tonumber(redis.call('HGET', KEYS[3], userId) or '0')
      if currentCodes >= limit then return {err = 'Invite code limit reached'} end
      
      -- Generate a unique code (timestamp + random)
      local code = ''
      local attempts = 0
      while attempts < 10 do
        code = tostring(redis.call('TIME')[1]) .. '_' .. tostring(math.random(100000, 999999))
        if redis.call('HSETNX', KEYS[1], code, userId) == 1 then
          -- Store the minimum bump positions with the code
          redis.call('HSET', KEYS[4], code, minBumpPositions)
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
      this.inviteCodeLimit,
      minBumpPositions
    );

    if (typeof result === 'string') {
      return result;
    }

    if (result && typeof result === 'object' && 'err' in result) {
      throw new Error((result as { err: string }).err);
    }

    throw new Error('Unexpected response from Redis');
  }

  async useInviteCode(code: string, id: string, userData: UserData, bumpPositions: number): Promise<InsertResult> {
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
      position: await this.getPosition(id),
      already_existed: false
    };
  }

  async getInviteCodeCreator(code: string): Promise<string | null> {
    return await this.redis.hget(this.keys.inviteCodes, code);
  }

  async getInviteCodeUser(code: string): Promise<string | null> {
    return await this.redis.hget(this.keys.usedInviteCodes, code);
  }

  async getUserInviteCodeCount(userId: string): Promise<number> {
    const count = await this.redis.hget(this.keys.userInviteCodes, userId);
    return count ? parseInt(count) : 0;
  }

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

  async getPositionAfterInviteCodeUseByEmail(email: string, code: string): Promise<number | null> {
    const userId = await this.getEmailMapping(email);
    if (!userId) {
      throw new Error('User not found with this email');
    }
    return await this.getPositionAfterInviteCodeUse(code);
  }

  async getPositionAfterInviteCodeUseByPhone(phone: string, code: string): Promise<number | null> {
    const userId = await this.getPhoneMapping(phone);
    if (!userId) {
      throw new Error('User not found with this phone');
    }
    return await this.getPositionAfterInviteCodeUse(code);
  }

  async getInviteCodeBumpPositions(code: string): Promise<number> {
    const positions = await this.redis.hget(this.keys.inviteCodeBumpPositions, code);
    return positions ? parseInt(positions) : 0;
  }
}