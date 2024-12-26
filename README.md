# Redis User Waitlist

A TypeScript implementation of a scalable waitlist system using Redis, supporting concurrent operations, user management, and position manipulation. Built with atomic operations using Redis Lua scripts for reliability at scale.

## Features

- Concurrent user management with email/phone identification
- Linked list-based waitlist system for efficient position management
- Invite code system with configurable position bumping
- Atomic operations using Redis Lua scripts
- Email and phone number uniqueness enforcement
- Comprehensive test suite with 25+ test scenarios

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
await waitlist.moveUser('user123', 5);
```

### API Methods

#### User Management
- `insertUser(id: string, data: UserData)`: Add a user with email/phone
- `getPosition(id: string)`: Get current position of a user
- `moveUser(id: string, targetPosition: number)`: Move user to a specific position
- `moveUserByEmail(email: string, targetPosition: number)`: Move user by email
- `moveUserByPhone(phone: string, targetPosition: number)`: Move user by phone
- `attachEmail(id: string, email: string)`: Add/update email for existing user
- `attachPhone(id: string, phone: string)`: Add/update phone for existing user
- `deleteUser(id: string)`: Remove user by ID
- `deleteUserByEmail(email: string)`: Remove user by email
- `deleteUserByPhone(phone: string)`: Remove user by phone

#### Invite Code System
- `createInviteCode(userId: string, minBumpPositions: number)`: Create invite code
- `useInviteCode(code: string, id: string, userData: UserData, bumpPositions: number)`: Use invite code
- `getInviteCodeCreator(code: string)`: Get code creator
- `getInviteCodeUser(code: string)`: Get user who used the code
- `getUserInviteCodeCount(userId: string)`: Get number of codes created by user
- `getPositionAfterInviteCodeUse(code: string)`: Preview position after using code
- `getInviteCodeBumpPositions(code: string)`: Get minimum bump positions for code

#### Waitlist Information
- `getOrderedIds()`: Get all user IDs in waitlist order
- `getEmailMapping(email: string)`: Get user ID by email
- `getPhoneMapping(phone: string)`: Get user ID by phone
- `getLength()`: Get total number of users in waitlist

## Technical Details

### Linked List Implementation

The waitlist uses a Redis list data structure for efficient position management:
- O(1) insertions at head/tail
- O(n) position-based insertions
- Atomic operations for all position changes
- No position rebalancing needed
- Supports unlimited users

### Atomic Operations

All critical operations use Redis Lua scripts to ensure atomicity and consistency:
- User insertion with duplicate checking
- Position updates and movement
- Contact information management
- Invite code creation and usage
- Deletion across multiple indices

### Data Structure

Uses multiple Redis data structures for efficient operations:
- List: Position management (`waitlist:list`)
- Hash: User data storage (`waitlist:users`)
- Hash: Email index (`waitlist:emails`)
- Hash: Phone index (`waitlist:phones`)
- Hash: Invite code management (`waitlist:invite_codes`, `waitlist:used_codes`)

## Testing

The test suite in `waitlistTests.ts` includes 25+ comprehensive test scenarios:

### Concurrent Operations
- Multiple user insertions
- Duplicate handling
- Position manipulation
- Invite code usage
- Contact information updates

### Scale Testing
- Large-scale operations (10,000+ users)
- Batch insertions and moves
- Repeated position changes
- Complex concurrent scenarios

### Edge Cases
- Position boundary conditions
- Deletion during movement
- Invite code limits
- Contact information uniqueness

Run the test suite:
```bash
yarn test
```

## License

MIT License - see the [LICENSE](LICENSE) file for details.