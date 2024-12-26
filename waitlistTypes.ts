export interface RedisConfig {
    host: string;
    port: number;
    password: string;
}

export interface InsertResult {
    id: string;
    position: number;
    already_existed: boolean;
  }
  
 export interface TrackedUser {
    id: string;
    email?: string;
    phone?: string;
  }

export interface UserData {
    email?: string;
    phone?: string;
    metadata?: Record<string, any>;
}