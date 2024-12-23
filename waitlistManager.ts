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
      local score = (count + 1) * 1000000
      
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
      
      -- Get current position and all members
      local currentRank = redis.call('ZRANK', KEYS[1], id)
      if not currentRank then return 0 end
      
      -- Get all members and their scores
      local members = redis.call('ZRANGE', KEYS[1], 0, -1, 'WITHSCORES')
      local scores = {}
      for i = 2, #members, 2 do
        scores[#scores + 1] = tonumber(members[i])
      end
      
      -- Calculate new score for insertion
      local newScore
      if targetPos == 1 then
        -- Moving to front
        newScore = scores[1] - 1000000
      else
        -- Get scores around target position
        local beforeScore = scores[targetPos - 1]
        local atScore = scores[targetPos]
        -- Place between them
        newScore = beforeScore + ((atScore - beforeScore) / 2)
      end
      
      redis.call('ZADD', KEYS[1], newScore, id)
      return 1
    `;

    return await this.redis.eval(script, 1, this.keys.waitlist, id, targetPosition) === 1;
  }
 
  async getPosition(id: string): Promise<number> {
    const rank = await this.redis.zrank(this.keys.waitlist, id);
    return rank === null ? 0 : rank + 1;
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
 
  async disconnect(): Promise<void> {
    await this.redis.quit();
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