# Redis User Waitlist

A TypeScript implementation of a scalable waitlist system using Redis, supporting concurrent operations, user management, and position manipulation. Built with atomic operations using Redis Lua scripts for reliability at scale.

## Features

- Concurrent user management with email/phone identification
- Linked list-based waitlist system for efficient position management
- Invite code system with configurable position bumping
- Community code system for direct signup access
- Signup cutoff management for controlled user signups
- Atomic operations using Redis Lua scripts
- Email and phone number uniqueness enforcement
- Comprehensive test suite with 45+ test scenarios

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
const result = await waitlist.insertUser({
  email: 'user@example.com',
  phone: '+1234567890', // optional
  metadata: { name: 'John Doe' }
});

// Get user position
const position = await waitlist.getPosition(result.id);

// Move user to a different position
await waitlist.moveUser(result.id, 5);
```

### API Methods

#### User Management
- `insertUser(data: UserData)`: Add a user with email/phone
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
- `useInviteCode(code: string, userData: UserData, bumpPositions: number)`: Use invite code
- `getInviteCodeCreator(code: string)`: Get code creator
- `getInviteCodeUser(code: string)`: Get user who used the code
- `getUserInviteCodeCount(userId: string)`: Get number of codes created by user
- `getPositionAfterInviteCodeUse(code: string)`: Preview position after using code
- `getInviteCodeBumpPositions(code: string)`: Get minimum bump positions for code
- `getUserCreatedInviteCodes(userId: string)`: Get all codes created by a user
- `getInviteCodeUsedBy(userId: string)`: Get invite code info used by a user

#### Community Code System
- `createCommunityCode(code: string, maxUses: number)`: Create a community code with usage limit
- `useCommunityCode(code: string, userData: UserData)`: Use code to sign up immediately
- `getCommunityCodeInfo(code: string)`: Get code usage information
- `deleteCommunityCode(code: string)`: Delete a community code

#### Signup Management
- `setSignupCutoff(cutoff: number)`: Set position cutoff for signups
- `isUserSignedUp(id: string)`: Check if user has signed up
- `canUserSignUp(id: string)`: Check if user is eligible to sign up
- `markUserAsSignedUp(id: string)`: Mark user as signed up

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
- Community code usage tracking
- Deletion across multiple indices
- Signup status management

### Data Structure

Uses multiple Redis data structures for efficient operations:
- List: Position management (`waitlist:list`)
- Hash: User data storage (`waitlist:users`)
- Hash: Email index (`waitlist:emails`)
- Hash: Phone index (`waitlist:phones`)
- Hash: Invite code management (`waitlist:invite_codes`, `waitlist:used_codes`)
- Hash: Community code management (`waitlist:codes`, `waitlist:codes:uses`)
- Set: Signup tracking (`waitlist:signed_up`)

## Testing

The test suite in `waitlistTests.ts` includes 45+ comprehensive test scenarios:

### Concurrent Operations
- Multiple user insertions
- Duplicate handling
- Position manipulation
- Invite code usage
- Community code usage
- Contact information updates
- Signup management

### Scale Testing
- Large-scale operations (100,000+ users)
- Batch insertions and moves
- Repeated position changes
- Complex concurrent scenarios
- Signup cutoff changes
- Community code usage limits

### Edge Cases
- Position boundary conditions
- Deletion during movement
- Invite code limits
- Community code usage tracking
- Contact information uniqueness
- Signup eligibility checks
- Cutoff boundary testing

Run the test suite:
```bash
yarn test
```

## License

MIT License - see the [LICENSE](LICENSE) file for details.