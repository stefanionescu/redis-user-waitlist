# Redis User Waitlist

A TypeScript implementation of a scalable waitlist system using Redis, supporting concurrent operations, user management, and position manipulation. Built with atomic operations using Redis Lua scripts for reliability at scale.

## Features

- Concurrent user management with email/phone identification
- Position-based waitlist system with 10M-point gaps
- Support for precise position control and user bumping
- Atomic operations using Redis Lua scripts
- Email and phone number uniqueness enforcement
- Comprehensive test suite with concurrent operation testing

## Prerequisites

- Node.js (v14 or higher)
- Redis server
- Yarn package manager

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/stefanionescu/redis-user-waitlist.git
   cd redis-user-waitlist
   ```

2. Install dependencies:
   ```bash
   yarn install
   ```

3. Configure environment:
   ```bash
   cp .env.example .env
   ```
   Edit `.env` with your Redis configuration.

## Usage

### Basic Example

```typescript
import { WaitlistManager } from './waitlistManager';

const waitlist = new WaitlistManager({
  host: process.env.REDIS_HOST || 'localhost',
  port: parseInt(process.env.REDIS_PORT || '6379'),
  password: process.env.REDIS_PASSWORD || '',
});

// Insert a user
const result = await waitlist.insertUser('user123', {
  email: 'user@example.com',
  phone: '+1234567890', // optional
  metadata: { name: 'John Doe' }
});

// Get user position
const position = await waitlist.getPosition('user123');

// Move user to a different position
await waitlist.bumpUserUp('user123', 1);
```

### API Methods

- `insertUser(id: string, data: UserData)`: Add a user with email/phone
- `getPosition(id: string)`: Get current position of a user
- `bumpUserUp(id: string, targetPosition: number)`: Move user to a specific position
- `attachEmail(id: string, email: string)`: Add/update email for existing user
- `attachPhone(id: string, phone: string)`: Add/update phone for existing user
- `deleteUser(id: string)`: Remove user by ID
- `deleteUserByEmail(email: string)`: Remove user by email
- `deleteUserByPhone(phone: string)`: Remove user by phone

## Technical Details

### Scoring System

The waitlist uses a gap scoring system where:
- Initial positions are spaced by 10,000,000 points
- When bumping users, scores are calculated as midpoints between positions
- This allows for virtually unlimited position insertions without rebalancing
- Supports up to 10,000+ users with precise position control

### Atomic Operations

All critical operations use Redis Lua scripts to ensure atomicity and consistency:
- User insertion with duplicate checking
- Position updates and bumping
- Contact information management
- Deletion across multiple indices

### Data Structure

Uses multiple Redis data structures for efficient operations:
- Sorted Set: Position management (`waitlist:scores`)
- Hash: User data storage (`waitlist:users`)
- Hash: Email index (`waitlist:emails`)
- Hash: Phone index (`waitlist:phones`)

## Testing

Run the comprehensive test suite:
```bash
yarn test
```

The test suite includes:
- Concurrent user operations (up to 10,000 users)
- Duplicate user handling
- Position manipulation and bumping
- Large-scale operations
- Contact information management
- Edge cases and error conditions
- Deletion and cleanup verification

## License

MIT License - see the [LICENSE](LICENSE) file for details.