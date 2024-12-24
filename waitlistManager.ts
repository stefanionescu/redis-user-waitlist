import { Redis } from 'ioredis';
import { InsertResult, RedisConfig, UserData } from './waitlistTypes';

export class WaitlistManager {
  protected redis: Redis;
  protected keys = {
    waitlist: 'waitlist:scores',
    users: 'waitlist:users',
    emails: 'waitlist:emails',
    phones: 'waitlist:phones'
  };
 
  constructor(config: RedisConfig) {
    this.redis = new Redis(config);
  }
 
  async insertUser(id: string, data: UserData): Promise<InsertResult> {
    if (!data.email && !data.phone) {
      throw new Error('Either email or phone must be provided');
    }

    const script = `
      local id = ARGV[1]
      local email = ARGV[2]
      local phone = ARGV[3]
      
      -- Check if user exists by email
      if email ~= '' then
        local existingIdByEmail = redis.call('HGET', KEYS[2], email)
        if existingIdByEmail then
          local score = redis.call('ZSCORE', KEYS[1], existingIdByEmail)
          return {score, 1, existingIdByEmail}
        end
      end
      
      -- Check if user exists by phone
      if phone ~= '' then
        local existingIdByPhone = redis.call('HGET', KEYS[3], phone)
        if existingIdByPhone then
          local score = redis.call('ZSCORE', KEYS[1], existingIdByPhone)
          return {score, 1, existingIdByPhone}
        end
      end
      
      -- Simple incrementing score with large gaps
      local count = redis.call('ZCARD', KEYS[1])
      local score = (count + 1) * 10000000
      
      redis.call('ZADD', KEYS[1], score, id)
      if email ~= '' then
        redis.call('HSET', KEYS[2], email, id)
      end
      if phone ~= '' then
        redis.call('HSET', KEYS[3], phone, id)
      end
      return {score, 0, id}
    `;
 
    const [score, exists, existingId] = await this.redis.eval(
      script, 
      3, 
      this.keys.waitlist, 
      this.keys.emails, 
      this.keys.phones, 
      id, 
      data.email || '', 
      data.phone || ''
    ) as [number, number, string];

    const userId = exists === 1 ? existingId : id;
    await this.redis.hset(this.keys.users, userId, JSON.stringify({
      id: userId,
      email: data.email,
      phone: data.phone,
      metadata: data.metadata
    }));

    return {position: await this.getPosition(userId), already_existed: exists === 1};
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

  async bumpUserUp(id: string, targetPosition: number): Promise<boolean> {
    const script = `
      local id, targetPos = ARGV[1], tonumber(ARGV[2])
      local SCORE_GAP = 10000000
      local lockKey = KEYS[1] .. ':lock'
      
      if redis.call('SET', lockKey, '1', 'NX', 'PX', 1000) == false then
        return 0
      end

      local currentRank = redis.call('ZRANK', KEYS[1], id)
      if not currentRank then 
        redis.call('DEL', lockKey)
        return 0 
      end

      -- Get target scores based on direction
      local scores
      if targetPos == 1 then
        scores = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
      elseif currentRank + 1 < targetPos then
        -- Moving backwards (down the list): get target-1 and target
        scores = redis.call('ZRANGE', KEYS[1], targetPos - 1, targetPos, 'WITHSCORES')
      else
        -- Moving forwards (up the list): get target-2 and target-1
        scores = redis.call('ZRANGE', KEYS[1], targetPos - 2, targetPos - 1, 'WITHSCORES')
      end

      local newScore
      if targetPos == 1 then
        newScore = tonumber(scores[2]) - SCORE_GAP
      else
        local beforeScore = tonumber(scores[2])
        local atScore = tonumber(scores[4])
        newScore = beforeScore + ((atScore - beforeScore) / 2)
      end
      
      redis.call('ZADD', KEYS[1], newScore, id)
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
 
  async deleteUser(id: string): Promise<boolean> {
    const data = await this.redis.hget(this.keys.users, id);
    if (!data) return false;
    
    const {email, phone} = JSON.parse(data);
    await this.redis.multi()
      .zrem(this.keys.waitlist, id)
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
      redis.call('ZREM', KEYS[1], userId)
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
      redis.call('ZREM', KEYS[1], userId)
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
    const rank = await this.redis.zrank(this.keys.waitlist, id);
    return rank === null ? 0 : rank + 1;
  }
 
  async _getOrderedIds(): Promise<string[]> {
    return await this.redis.zrange(this.keys.waitlist, 0, -1);
  }
 
  async _getEmailMapping(email: string): Promise<string | null> {
    return await this.redis.hget(this.keys.emails, email);
  }

  async _getPhoneMapping(phone: string): Promise<string | null> {
    return await this.redis.hget(this.keys.phones, phone);
  }
}