# Redis User Waitlist

A TypeScript implementation of a scalable waitlist system using Redis, supporting concurrent operations, user management, and position manipulation.

## Features

- Concurrent user management with email/phone identification
- Position-based waitlist system with gap scoring
- Support for bumping users to different positions
- Atomic operations using Redis Lua scripts
- Comprehensive test suite for all operations

## Prerequisites

- Node.js (v14 or higher)
- Redis server
- Yarn or npm

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
metadata: { name: 'John Doe' }
});
// Get user position
const position = await waitlist.getPosition('user123');
// Bump user to a different position
await waitlist.bumpUserUp('user123', 1);
```

### Key Operations

- `insertUser`: Add a user to the waitlist
- `getPosition`: Get current position of a user
- `bumpUserUp`: Move a user to a higher position
- `attachEmail`: Add email to existing user
- `attachPhone`: Add phone to existing user
- `deleteUser`: Remove user from waitlist

## Testing

Run the comprehensive test suite:
```bash
yarn test
```


The test suite includes:
- Concurrent user insertion
- Duplicate user handling
- Position manipulation
- Large-scale operations
- Email/phone attachment
- Edge cases

## Technical Details

### Scoring System

The waitlist uses a gap scoring system where:
- Initial positions are spaced by 1,000,000 points
- When bumping users, scores are calculated as midpoints between existing positions
- This allows for virtually unlimited position insertions without rebalancing

### Atomic Operations

All critical operations use Redis Lua scripts to ensure atomicity and consistency, particularly important for:
- User insertion with duplicate checking
- Position updates
- Contact information management

## License

MIT License - see the [LICENSE](LICENSE) file for details.