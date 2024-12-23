export interface RedisConfig {
    host: string;
    port: number;
    password: string;
}

export interface InsertResult {
    position: number;
    already_existed: boolean;
}

export interface UserData {
    email?: string;
    phone?: string;
    metadata?: Record<string, any>;
}