// server.js
require('dotenv').config();

const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');
const fs = require('fs');
const mongoose = require('mongoose');
const { Redis } = require('@upstash/redis');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const cookieParser = require('cookie-parser');

const app = express();

app.use(express.json({ limit: '100mb' }));
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));

const JWT_SECRET =
  process.env.JWT_SECRET || 'local_chat_app_development_secret_key_987654321';

const localDataFile = path.join(__dirname, 'local-data.json');
const EPHEMERAL_TTL_MS = 24 * 60 * 60 * 1000;
const EPHEMERAL_CLEANUP_INTERVAL_MS = 5 * 60 * 1000;
const CHAT_THEME_IDS = [
  'pink-dream',
  'love-bouquet',
  'tulip-soft',
  'soft-heart',
  'pink-hearts',
  'kawaii-pink'
];
const LEGACY_CHAT_THEME_MAP = {
  aurora: 'love-bouquet',
  linen: 'tulip-soft',
  glass: 'pink-hearts',
  garden: 'kawaii-pink',
  midnight: 'soft-heart',
  'pink-pop': 'love-bouquet',
  rose: 'pink-hearts',
  'cotton-candy': 'tulip-soft',
  lavender: 'kawaii-pink',
  ocean: 'pink-hearts'
};
const ALLOWED_CHAT_THEMES = new Set([
  ...CHAT_THEME_IDS,
  ...Object.keys(LEGACY_CHAT_THEME_MAP)
]);

function normalizeChatTheme(theme) {
  return CHAT_THEME_IDS.includes(theme)
    ? theme
    : (LEGACY_CHAT_THEME_MAP[theme] || 'pink-dream');
}

function saveLocalDbData() {
  try {
    fs.writeFileSync(
      localDataFile,
      JSON.stringify(
        {
          users: localDbMock.users,
          messages: localDbMock.messages,
          groups: localDbMock.groups,
          readReceipts: localDbMock.readReceipts,
          reports: localDbMock.reports,
          roomSettings: localDbMock.roomSettings
        },
        null,
        2
      )
    );
  } catch (err) {
    console.warn('[LOCAL DB SAVE ERROR]', err.message);
  }
}

function loadLocalDbData() {
  try {
    if (!fs.existsSync(localDataFile)) return;

    const savedData = JSON.parse(fs.readFileSync(localDataFile, 'utf8'));

    localDbMock.users = savedData.users || [];
    localDbMock.messages = savedData.messages || [];
    localDbMock.groups = savedData.groups || [];
    localDbMock.readReceipts = savedData.readReceipts || {};
    localDbMock.reports = savedData.reports || [];
    localDbMock.roomSettings = savedData.roomSettings || {};

    console.log('[LOCAL DB] Loaded local-data.json.');
  } catch (err) {
    console.warn('[LOCAL DB LOAD ERROR]', err.message);
  }
}

function toGroupListItem(group) {
  if (!group) return null;

  const source = typeof group.toObject === 'function'
    ? group.toObject()
    : group;

  return {
    _id: source._id,
    name: source.name,
    creator: source.creator,
    members: source.members || [],
    avatarThumb: source.avatarThumb || '',
    createdAt: source.createdAt
  };
}

let isMongoConnected = false;
let isRedisConnected = false;
let mongoStatus = 'not_configured';
let redisStatus = 'not_configured';
let mongoLastError = '';
const avatarCache = new Map();

// ================================
// LOCAL DATABASE MOCK
// ================================
const localDbMock = {
  users: [],
  messages: [],
  groups: [],
  readReceipts: {},
  reports: [],
  roomSettings: {},

  async findUser(username) {
    return this.users.find(
      u => u.username.toLowerCase() === username.toLowerCase()
    );
  },

  async getAllUsers() {
    return this.users.map(u => ({
      id: u._id,
      username: u.username,
      displayName: u.displayName || u.username,
      avatar: u.avatar,
      coverPhoto: u.coverPhoto || '',
      bio: u.bio || 'Infinity Chat user',
      aboutMe: u.aboutMe || '',
      customStatus: u.customStatus || '',
      story: u.story || null,
      stories: u.stories || [],
      note: u.note || null,
      profileTheme: u.profileTheme || 'neon-pink',
      chatBackground: u.chatBackground || 'pink-dream',
      privacy: u.privacy || {},
      lastSeen: u.lastSeen || u.createdAt,
      createdAt: u.createdAt
    }));
  },

  async createUser(username, hashedPassword, avatar) {
    const newUser = {
      _id: 'mock_u_' + Math.random().toString(36).substr(2, 9),
      username,
      password: hashedPassword,
      avatar:
        avatar ||
        `https://api.dicebear.com/7.x/bottts/svg?seed=${username}&backgroundColor=ff2da6,f472b6,c026d3`,
      displayName: username,
      coverPhoto: '',
      bio: 'Infinity Chat user',
      aboutMe: '',
      customStatus: '',
      story: null,
      stories: [],
      note: null,
      profileTheme: 'neon-pink',
      chatBackground: 'pink-dream',
      privacy: {
        showOnlineStatus: true,
        showLastSeen: true,
        allowDirectMessages: true,
        allowProfileViewing: true
      },
      lastSeen: new Date(),
      createdAt: new Date()
    };

    this.users.push(newUser);
    saveLocalDbData();
    return newUser;
  },

  async updateUserProfile(userId, profileData) {
    const user = this.users.find(u => u._id === userId);

    if (!user) return null;

    Object.assign(user, profileData);

    saveLocalDbData();
    return user;
  },

  async updateLastSeen(username) {
    const user = this.users.find(u => u.username === username);

    if (user) {
      user.lastSeen = new Date();
      saveLocalDbData();
    }

    return user;
  },

  async getRecentMessages(roomId = 'lounge', limit = 50) {
    return this.messages.filter(m => m.roomId === roomId).slice(-limit);
  },

  async saveMessage(sender, content, type = 'chat', roomId = 'lounge', replyTo = null) {
    const user = this.users.find(u => u.username === sender);

    const newMsg = {
      _id: 'mock_m_' + Math.random().toString(36).substr(2, 9),
      type,
      roomId,
      username: sender,
      sender,
      avatar: user?.avatar || '',
      message: content,
      content,
      replyTo,
      reactions: {},
      deletedFor: [],
      deletedForEveryone: false,
      status: 'sent',
      timestamp: new Date()
    };

    this.messages.push(newMsg);
    saveLocalDbData();
    return newMsg;
  },

  async createGroup(name, creator, members = [], avatar) {
    const newGroup = {
      _id: 'mock_g_' + Math.random().toString(36).substr(2, 9),
      name,
      creator,
      members: Array.from(new Set([creator, ...members])),
      avatar:
        avatar ||
        `https://api.dicebear.com/7.x/initials/svg?seed=${encodeURIComponent(
          name
        )}&backgroundColor=ff2da6,c026d3&fontColor=ffffff`,
      avatarThumb:
        avatar ||
        `https://api.dicebear.com/7.x/initials/svg?seed=${encodeURIComponent(
          name
        )}&backgroundColor=ff2da6,c026d3&fontColor=ffffff`,
      createdAt: new Date()
    };

    this.groups.push(newGroup);
    saveLocalDbData();
    return newGroup;
  },

  async getGroupsForUser(username) {
    return this.groups.filter(g => g.members.includes(username));
  },

  async leaveGroup(groupId, username) {
    const group = this.groups.find(g => g._id === groupId);

    if (group) {
      group.members = group.members.filter(m => m !== username);
      saveLocalDbData();
    }

    return group;
  },

  async updateGroupMembers(groupId, members) {
    const group = this.groups.find(g => g._id === groupId);

    if (group) {
      group.members = Array.from(new Set([group.creator, ...members]));
      saveLocalDbData();
    }

    return group;
  },

  async updateGroupInfo(groupId, updates = {}) {
    const group = this.groups.find(g => g._id === groupId);

    if (group) {
      if (updates.name) group.name = updates.name;
      if (updates.avatar) group.avatar = updates.avatar;
      if (updates.avatarThumb) group.avatarThumb = updates.avatarThumb;
      saveLocalDbData();
    }

    return group;
  },

  async clearRoomMessages(roomId) {
    this.messages = this.messages.filter(message => message.roomId !== roomId);

    if (this.readReceipts?.[roomId]) {
      delete this.readReceipts[roomId];
    }

    saveLocalDbData();
  },

  async saveReport(report) {
    this.reports.push({
      ...report,
      _id: 'mock_r_' + Math.random().toString(36).substr(2, 9),
      createdAt: new Date()
    });
    saveLocalDbData();
  },

  async setMessageReaction(messageId, username, emoji) {
    const message = this.messages.find(m => m._id === messageId);

    if (!message) return null;

    message.reactions = message.reactions || {};

    Object.keys(message.reactions).forEach(key => {
      message.reactions[key] = message.reactions[key].filter(
        member => member !== username
      );

      if (message.reactions[key].length === 0) {
        delete message.reactions[key];
      }
    });

    if (emoji) {
      if (!message.reactions[emoji]) message.reactions[emoji] = [];
      message.reactions[emoji].push(username);
    }

    saveLocalDbData();
    return message;
  }
};

loadLocalDbData();

// ================================
// MONGODB SETUP
// ================================
const DEFAULT_MONGO_DB_NAME = 'infinity-chat';

function buildMongoConfig(rawUri) {
  if (!rawUri) {
    return {
      uri: '',
      dbName: DEFAULT_MONGO_DB_NAME
    };
  }

  try {
    const dbMatch = rawUri.match(/^mongodb(?:\+srv)?:\/\/[^/]+\/([^?]+)/i);

    if (dbMatch?.[1]) {
      return {
        uri: rawUri,
        dbName: decodeURIComponent(dbMatch[1]) || DEFAULT_MONGO_DB_NAME
      };
    }

    const parsedUri = new URL(rawUri);
    const uriDbName = parsedUri.pathname.replace(/^\/+/, '').trim();
    const dbName = uriDbName || DEFAULT_MONGO_DB_NAME;

    if (!uriDbName) {
      parsedUri.pathname = `/${DEFAULT_MONGO_DB_NAME}`;
    }

    return {
      uri: parsedUri.toString(),
      dbName
    };
  } catch (err) {
    console.warn(
      '[DATABASE] Could not normalize MONGODB_URI. Using it as provided.',
      err.message
    );

    return {
      uri: rawUri,
      dbName: DEFAULT_MONGO_DB_NAME
    };
  }
}

const { uri: mongoUri, dbName: mongoDbName } = buildMongoConfig(
  process.env.MONGODB_URI
);

let User;
let Message;
let Group;
let ReadReceipt;
let RoomSetting;
let mongoConnectPromise = Promise.resolve();
let ephemeralCleanupTimer = null;

if (mongoUri) {
  const userSchema = new mongoose.Schema({
    username: {
      type: String,
      required: true,
      unique: true,
      lowercase: true,
      trim: true
    },
    password: { type: String, required: true },
    avatar: { type: String },
    coverPhoto: { type: String, default: '' },
    displayName: { type: String, trim: true },
    bio: { type: String, default: 'Infinity Chat user' },
    aboutMe: { type: String, default: '' },
    customStatus: { type: String, default: '' },
    story: {
      type: {
        type: String,
        enum: ['text', 'image'],
        default: 'text'
      },
      content: { type: String, default: '' },
      createdAt: { type: Date },
      views: [{
        username: { type: String },
        avatar: { type: String },
        viewedAt: { type: Date, default: Date.now }
      }],
      reactions: [{
        username: { type: String },
        avatar: { type: String },
        type: { type: String },
        reactedAt: { type: Date, default: Date.now }
      }]
    },
    stories: [{
      id: { type: String },
      type: {
        type: String,
        enum: ['note', 'image', 'video'],
        default: 'note'
      },
      content: { type: String, default: '' },
      mediaUrl: { type: String, default: '' },
      createdAt: { type: Date },
      expiresAt: { type: Date },
      views: [{
        username: { type: String },
        avatar: { type: String },
        viewedAt: { type: Date, default: Date.now }
      }],
      reactions: [{
        username: { type: String },
        avatar: { type: String },
        type: { type: String },
        reactedAt: { type: Date, default: Date.now }
      }]
    }],
    note: {
      text: { type: String, default: '' },
      updatedAt: { type: Date }
    },
    profileTheme: {
      type: String,
      default: 'neon-pink',
      enum: ['neon-pink', 'purple', 'blue', 'green', 'dark']
    },
    chatBackground: {
      type: String,
      default: 'pink-dream',
      enum: Array.from(ALLOWED_CHAT_THEMES)
    },
    privacy: {
      showOnlineStatus: { type: Boolean, default: true },
      showLastSeen: { type: Boolean, default: true },
      allowDirectMessages: { type: Boolean, default: true },
      allowProfileViewing: { type: Boolean, default: true }
    },
    lastSeen: { type: Date, default: Date.now },
    createdAt: { type: Date, default: Date.now }
  });

  userSchema.index({ 'story.content': 1 });
  userSchema.index({ 'note.text': 1 });

  const messageSchema = new mongoose.Schema({
    sender: { type: String, required: true },
    content: { type: String, required: true },
    type: { type: String, default: 'chat' },
    roomId: { type: String, default: 'lounge', index: true },
    replyTo: {
      messageId: { type: String, default: '' },
      username: { type: String, default: '' },
      content: { type: String, default: '' },
      roomId: { type: String, default: '' }
    },
    reactions: {
      type: Map,
      of: [String],
      default: {}
    },
    deletedFor: [{ type: String }],
    deletedForEveryone: { type: Boolean, default: false },
    status: { type: String, default: 'sent' },
    timestamp: { type: Date, default: Date.now }
  });

  const groupSchema = new mongoose.Schema({
    name: { type: String, required: true },
    creator: { type: String, required: true },
    members: [{ type: String }],
    avatar: { type: String },
    avatarThumb: { type: String },
    createdAt: { type: Date, default: Date.now }
  });

  groupSchema.index({ members: 1 });
  groupSchema.index({ creator: 1 });

  const readReceiptSchema = new mongoose.Schema({
    roomId: { type: String, required: true, index: true },
    username: { type: String, required: true, index: true },
    avatar: { type: String, default: '' },
    lastMessageId: { type: String, required: true },
    updatedAt: { type: Date, default: Date.now }
  });

  readReceiptSchema.index({ roomId: 1, username: 1 }, { unique: true });

  const roomSettingSchema = new mongoose.Schema({
    roomId: { type: String, required: true, unique: true, index: true },
    type: { type: String, enum: ['dm'], default: 'dm' },
    members: [{ type: String, index: true }],
    chatTheme: {
      type: String,
      default: 'pink-dream',
      enum: CHAT_THEME_IDS
    },
    updatedBy: { type: String },
    updatedAt: { type: Date, default: Date.now }
  });

  User = mongoose.model('User', userSchema);
  Message = mongoose.model('Message', messageSchema);
  Group = mongoose.model('Group', groupSchema);
  ReadReceipt = mongoose.model('ReadReceipt', readReceiptSchema);
  RoomSetting = mongoose.model('RoomSetting', roomSettingSchema);

  mongoConnectPromise = mongoose
    .connect(mongoUri, {
      serverSelectionTimeoutMS: 30000,
      connectTimeoutMS: 30000,
      socketTimeoutMS: 60000,
      maxPoolSize: 10,
      minPoolSize: 0
    })
    .then(() => {
      isMongoConnected = true;
      mongoStatus = 'connected';
      mongoLastError = '';
      console.log(`[DATABASE] Successfully connected to MongoDB Atlas database: ${mongoose.connection.name}`);
    })
    .catch(err => {
      isMongoConnected = false;
      mongoStatus = 'fallback';
      mongoLastError = err.message;
      console.warn(
        '[DATABASE] MongoDB connection failed. Falling back to local-data.json.',
        err.message
      );
    });

  mongoose.connection.on('disconnected', () => {
    isMongoConnected = false;
    mongoStatus = 'disconnected';
    console.warn('[DATABASE] MongoDB disconnected. Using local fallback until it reconnects.');
  });

  mongoose.connection.on('reconnected', () => {
    isMongoConnected = true;
    mongoStatus = 'connected';
    mongoLastError = '';
    console.log('[DATABASE] MongoDB reconnected.');
  });
} else {
  console.warn(
    '[SYSTEM] MONGODB_URI is not defined. Falling back to local-data.json.'
  );
}

// ================================
// REDIS MOCK / SETUP
// ================================
const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;

let redis;
let redisConnectPromise = Promise.resolve();

const localRedisMock = {
  onlineUsers: new Set(),
  cachedMessages: {},

  async sadd(key, member) {
    this.onlineUsers.add(member);
    return 1;
  },

  async srem(key, member) {
    return this.onlineUsers.delete(member) ? 1 : 0;
  },

  async smembers(key) {
    return Array.from(this.onlineUsers);
  },

  async lrange(key, start, stop) {
    if (!this.cachedMessages[key]) return [];

    return this.cachedMessages[key].slice(
      start,
      stop === -1 ? undefined : stop + 1
    );
  },

  async rpush(key, value) {
    if (!this.cachedMessages[key]) this.cachedMessages[key] = [];

    this.cachedMessages[key].push(value);

    return this.cachedMessages[key].length;
  },

  async ltrim(key, start, stop) {
    if (!this.cachedMessages[key]) return 'OK';

    this.cachedMessages[key] = this.cachedMessages[key].slice(
      start,
      stop === -1 ? undefined : stop + 1
    );

    return 'OK';
  }
};

if (redisUrl && redisToken) {
  redisConnectPromise = (async () => {
    try {
      redis = new Redis({
        url: redisUrl,
        token: redisToken
      });

      await redis.ping();
      isRedisConnected = true;
      redisStatus = 'connected';
      console.log('[CACHE] Successfully connected to Upstash Redis!');
    } catch (err) {
      isRedisConnected = false;
      redisStatus = 'fallback';
      console.warn('[CACHE] Redis failed. Using local mock.', err.message);
    }
  })();
} else {
  console.warn('[SYSTEM] Redis credentials not found. Using local Redis mock.');
}

const getRedisClient = () => (isRedisConnected ? redis : localRedisMock);

// ================================
// AUTH MIDDLEWARE
// ================================
const authenticateToken = async (req, res, next) => {
  let token = req.cookies.token;

  if (!token && req.headers.authorization) {
    const authHeader = req.headers.authorization;

    if (authHeader.startsWith('Bearer ')) {
      token = authHeader.split(' ')[1];
    }
  }

  if (!token) {
    return res.status(401).json({
      error: 'Access denied. No token provided.'
    });
  }

  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    req.user = decoded;
    next();
  } catch (err) {
    return res.status(403).json({
      error: 'Invalid or expired authentication token.'
    });
  }
};

function createUserToken(user) {
  return jwt.sign(
    {
      id: user._id,
      username: user.username
    },
    JWT_SECRET,
    { expiresIn: '7d' }
  );
}

function sanitizeStoryItem(story) {
  if (!story || typeof story !== 'object') return null;

  const storyType = ['note', 'image', 'video'].includes(story.type)
    ? story.type
    : story.mediaUrl
      ? 'image'
      : 'note';
  const createdAt = story.createdAt ? new Date(story.createdAt) : new Date();
  const expiresAt = story.expiresAt
    ? new Date(story.expiresAt)
    : new Date(createdAt.getTime() + EPHEMERAL_TTL_MS);
  const mediaUrl = storyType === 'image' || storyType === 'video'
    ? String(story.mediaUrl || story.content || '').slice(0, storyType === 'video' ? 5000000 : 1200000)
    : '';
  const content = storyType === 'note'
    ? String(story.content || '').slice(0, 60)
    : String(story.content || '').slice(0, 120);

  if (storyType === 'note' && !content) return null;
  if ((storyType === 'image' || storyType === 'video') && !mediaUrl) return null;

  return {
    id: story.id || `story_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
    type: storyType,
    content,
    mediaUrl,
    createdAt,
    expiresAt,
    views: Array.isArray(story.views) ? story.views : [],
    reactions: Array.isArray(story.reactions) ? story.reactions : []
  };
}

function getActiveNote(note) {
  if (!note?.text) return null;

  const updatedAt = note.updatedAt ? new Date(note.updatedAt) : new Date();
  if (Number.isNaN(updatedAt.getTime())) return null;
  if (Date.now() - updatedAt.getTime() >= EPHEMERAL_TTL_MS) return null;

  return {
    text: String(note.text).slice(0, 60),
    updatedAt
  };
}

function getActiveStories(user) {
  const now = Date.now();
  const stories = Array.isArray(user?.stories)
    ? user.stories.map(sanitizeStoryItem).filter(Boolean)
    : [];
  const activeStories = stories.filter(story => new Date(story.expiresAt).getTime() > now);

  if (activeStories.length) return activeStories;

  if (user?.story?.content && user.story.createdAt) {
    const createdAt = new Date(user.story.createdAt);
    const expiresAt = new Date(createdAt.getTime() + EPHEMERAL_TTL_MS);

    if (expiresAt.getTime() > now) {
      return [{
        id: user.story.id || 'legacy_story',
        type: user.story.type === 'image' ? 'image' : 'note',
        content: user.story.type === 'image' ? '' : String(user.story.content || '').slice(0, 60),
        mediaUrl: user.story.type === 'image' ? user.story.content : '',
        createdAt,
        expiresAt,
        views: user.story.views || [],
        reactions: user.story.reactions || []
      }];
    }
  }

  return [];
}

function cleanUserExpiringContent(user) {
  if (!user) return false;

  let changed = false;
  const activeStories = getActiveStories(user).filter(story => story.id !== 'legacy_story');
  const hadStories = Array.isArray(user.stories) && user.stories.length > activeStories.length;

  if (Array.isArray(user.stories) && (hadStories || user.stories.length !== activeStories.length)) {
    user.stories = activeStories;
    changed = true;
  }

  if (user.story?.content) {
    const createdAt = user.story.createdAt ? new Date(user.story.createdAt) : null;
    const expired =
      !createdAt ||
      Number.isNaN(createdAt.getTime()) ||
      Date.now() - createdAt.getTime() >= EPHEMERAL_TTL_MS;

    if (expired) {
      user.story = null;
      changed = true;
    }
  }

  if (user.note?.text && !getActiveNote(user.note)) {
    user.note = null;
    changed = true;
  }

  return changed;
}

async function cleanupExpiredEphemeralContent() {
  try {
    let cleanedCount = 0;

    if (isMongoConnected) {
      const users = await User.find({
        $or: [
          { 'story.content': { $exists: true, $ne: '' } },
          { 'stories.0': { $exists: true } },
          { 'note.text': { $exists: true, $ne: '' } }
        ]
      });

      for (const user of users) {
        if (!cleanUserExpiringContent(user)) continue;

        user.markModified('story');
        user.markModified('stories');
        user.markModified('note');
        await user.save();
        cleanedCount += 1;
      }
    } else {
      localDbMock.users.forEach(user => {
        if (cleanUserExpiringContent(user)) cleanedCount += 1;
      });

      if (cleanedCount) saveLocalDbData();
    }

    if (cleanedCount) {
      console.log(`[SYSTEM] Removed expired notes/stories for ${cleanedCount} user(s).`);
    }

    return cleanedCount;
  } catch (err) {
    console.warn('[EPHEMERAL CLEANUP ERROR]', err.message);
    return 0;
  }
}

function getStoryTarget(user, storyId) {
  const activeStories = getActiveStories(user);
  const targetId = storyId || activeStories[0]?.id;

  if (!targetId) return null;

  if (Array.isArray(user.stories)) {
    const story = user.stories.find(item => item.id === targetId);
    if (story) return story;
  }

  if (targetId === 'legacy_story' && user.story?.content) return user.story;

  return null;
}

async function applyStoryReaction(ownerUsername, reactorUsername, reactorId, storyId, reactionType) {
  if (!ownerUsername || ownerUsername === reactorUsername || !reactionType) {
    return null;
  }

  let owner;
  let reactor;

  if (isMongoConnected) {
    owner = await User.findOne({ username: ownerUsername.toLowerCase() });
    reactor = await User.findById(reactorId);
  } else {
    owner = await localDbMock.findUser(ownerUsername);
    reactor = localDbMock.users.find(u => u._id === reactorId);
  }

  const story = getStoryTarget(owner, storyId);

  if (!story) {
    const error = new Error('Story not found.');
    error.statusCode = 404;
    throw error;
  }

  const reactions = (story.reactions || []).filter(
    reaction => reaction.username !== reactorUsername
  );

  reactions.push({
    username: reactorUsername,
    avatar: reactor?.avatar || '',
    type: String(reactionType).slice(0, 12),
    reactedAt: new Date()
  });

  story.reactions = reactions;

  if (isMongoConnected) {
    owner.markModified('stories');
    owner.markModified('story');
    await owner.save();
  } else {
    saveLocalDbData();
  }

  return userResponse(owner);
}

async function broadcastProfileUpdated(updatedUser) {
  try {
    if (!updatedUser?.username) return;

    const [userWithCounts] = await addMessageCountsToResponses([updatedUser]);
    avatarCache.set(userWithCounts.username.toLowerCase(), userWithCounts.avatar || '');

    const profilePayload = JSON.stringify({
      type: 'profile_updated',
      user: userWithCounts
    });

    wss.clients.forEach(client => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(profilePayload);
      }
    });
  } catch (err) {
    console.warn('[PROFILE BROADCAST ERROR]', err.message);
  }
}

function broadcastStoryReactionUpdated(ownerUsername, storyId, reaction) {
  const payload = JSON.stringify({
    type: 'story_reaction_updated',
    ownerUsername,
    storyId,
    reaction
  });

  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(payload);
    }
  });
}

function broadcastStoryViewUpdated(ownerUsername, storyId, view) {
  const payload = JSON.stringify({
    type: 'story_view_updated',
    ownerUsername,
    storyId,
    view
  });

  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(payload);
    }
  });
}

function broadcastStoryListUpdated(ownerUsername, stories) {
  const payload = JSON.stringify({
    type: 'story_list_updated',
    ownerUsername,
    stories
  });

  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(payload);
    }
  });
}

function userResponse(user) {
  if (user?.username) {
    avatarCache.set(String(user.username).toLowerCase(), user.avatar || '');
  }

  const activeStories = getActiveStories(user);
  const activeNote = getActiveNote(user.note);

  return {
    id: user._id,
    username: user.username,
    displayName: user.displayName || user.username,
    avatar: user.avatar,
    coverPhoto: user.coverPhoto || '',
    bio: user.bio || 'Infinity Chat user',
    aboutMe: user.aboutMe || '',
    customStatus: user.customStatus || '',
    story: activeStories[0] || null,
    stories: activeStories,
    note: activeNote,
    profileTheme: user.profileTheme || 'neon-pink',
    chatBackground: user.chatBackground || 'pink-dream',
    privacy: {
      showOnlineStatus: user.privacy?.showOnlineStatus !== false,
      showLastSeen: user.privacy?.showLastSeen !== false,
      allowDirectMessages: user.privacy?.allowDirectMessages !== false,
      allowProfileViewing: user.privacy?.allowProfileViewing !== false
    },
    lastSeen: user.lastSeen || user.createdAt,
    createdAt: user.createdAt
  };
}

async function getUserMessageCounts(usernames = []) {
  const normalizedNames = Array.from(
    new Set(
      usernames
        .filter(Boolean)
        .map(username => String(username).toLowerCase())
    )
  );

  const counts = new Map(normalizedNames.map(username => [username, 0]));
  if (!normalizedNames.length) return counts;

  if (isMongoConnected) {
    const rows = await Message.aggregate([
      {
        $match: {
          deletedForEveryone: { $ne: true }
        }
      },
      {
        $group: {
          _id: { $toLower: '$sender' },
          total: { $sum: 1 }
        }
      },
      {
        $match: {
          _id: { $in: normalizedNames }
        }
      }
    ]);

    rows.forEach(row => counts.set(String(row._id).toLowerCase(), row.total));
    return counts;
  }

  localDbMock.messages.forEach(message => {
    if (message.deletedForEveryone) return;

    const sender = String(message.sender || message.username || '').toLowerCase();
    if (!counts.has(sender)) return;

    counts.set(sender, counts.get(sender) + 1);
  });

  return counts;
}

async function addMessageCountsToResponses(usersList = []) {
  const counts = await getUserMessageCounts(usersList.map(user => user.username));

  return usersList.map(user => {
    const totalMessages = counts.get(String(user.username || '').toLowerCase()) || 0;

    return {
      ...user,
      messages: totalMessages,
      messageCount: totalMessages,
      totalMessages
    };
  });
}

// ================================
// AUTH ROUTES
// ================================
app.post('/api/auth/signup', async (req, res) => {
  try {
    let { username, password, avatar } = req.body;

    if (!username || !password) {
      return res.status(400).json({
        error: 'Username and password are required.'
      });
    }

    username = username.trim();

    if (username.length < 3 || username.length > 15) {
      return res.status(400).json({
        error: 'Username must be between 3 and 15 characters.'
      });
    }

    let existingUser;

    if (isMongoConnected) {
      existingUser = await User.findOne({
        username: username.toLowerCase()
      });
    } else {
      existingUser = await localDbMock.findUser(username);
    }

    if (existingUser) {
      return res.status(400).json({
        error: 'Username is already taken.'
      });
    }

    const salt = await bcrypt.genSalt(10);
    const hashedPassword = await bcrypt.hash(password, salt);

    if (!avatar) {
      avatar = `https://api.dicebear.com/7.x/bottts/svg?seed=${encodeURIComponent(
        username
      )}&backgroundColor=ff2da6,f472b6,c026d3`;
    }

    let user;

    if (isMongoConnected) {
      user = new User({
        username,
        password: hashedPassword,
        avatar,
        displayName: username,
        bio: 'Infinity Chat user',
        lastSeen: new Date()
      });

      await user.save();
    } else {
      user = await localDbMock.createUser(username, hashedPassword, avatar);
    }

    const token = createUserToken(user);

    res.cookie('token', token, {
      httpOnly: true,
      secure: process.env.NODE_ENV === 'production',
      maxAge: 7 * 24 * 60 * 60 * 1000
    });

    const [userPayload] = await addMessageCountsToResponses([userResponse(user)]);

    res.status(201).json({
      success: true,
      token,
      user: userPayload
    });
  } catch (error) {
    console.error('[SIGNUP ERROR]', error);

    res.status(500).json({
      error: 'An internal server error occurred during registration.'
    });
  }
});

app.post('/api/auth/login', async (req, res) => {
  try {
    let { username, password } = req.body;

    if (!username || !password) {
      return res.status(400).json({
        error: 'Username and password are required.'
      });
    }

    username = username.trim();

    let user;

    if (isMongoConnected) {
      user = await User.findOne({
        username: username.toLowerCase()
      });
    } else {
      user = await localDbMock.findUser(username);
    }

    if (!user) {
      return res.status(400).json({
        error: 'Invalid username or password.'
      });
    }

    const isMatch = await bcrypt.compare(password, user.password);

    if (!isMatch) {
      return res.status(400).json({
        error: 'Invalid username or password.'
      });
    }

    user.lastSeen = new Date();

    if (isMongoConnected) {
      await user.save();
    }

    const token = createUserToken(user);

    res.cookie('token', token, {
      httpOnly: true,
      secure: process.env.NODE_ENV === 'production',
      maxAge: 7 * 24 * 60 * 60 * 1000
    });

    const [userPayload] = await addMessageCountsToResponses([userResponse(user)]);

    res.status(200).json({
      success: true,
      token,
      user: userPayload
    });
  } catch (error) {
    console.error('[LOGIN ERROR]', error);

    res.status(500).json({
      error: 'An internal server error occurred during login.'
    });
  }
});

app.get('/api/auth/me', authenticateToken, async (req, res) => {
  try {
    let user;

    if (isMongoConnected) {
      user = await User.findById(req.user.id);
    } else {
      user = localDbMock.users.find(u => u._id === req.user.id);
    }

    if (!user) {
      return res.status(404).json({
        error: 'User not found.'
      });
    }

    const [userPayload] = await addMessageCountsToResponses([userResponse(user)]);

    res.status(200).json({
      success: true,
      user: userPayload
    });
  } catch (err) {
    res.status(500).json({
      error: 'Failed to get current user.'
    });
  }
});

app.post('/api/auth/logout', async (req, res) => {
  try {
    let token = req.cookies.token;

    if (!token && req.headers.authorization) {
      const authHeader = req.headers.authorization;

      if (authHeader.startsWith('Bearer ')) {
        token = authHeader.split(' ')[1];
      }
    }

    if (token) {
      const decoded = jwt.verify(token, JWT_SECRET);

      if (isMongoConnected) {
        await User.findByIdAndUpdate(decoded.id, {
          lastSeen: new Date()
        });
      } else {
        await localDbMock.updateLastSeen(decoded.username);
      }
    }
  } catch (err) {
    console.warn('[LOGOUT LASTSEEN ERROR]', err.message);
  }

  res.clearCookie('token');

  res.status(200).json({
    success: true,
    message: 'Logged out successfully.'
  });
});

// ================================
// PROFILE ROUTE
// ================================
app.put('/api/profile', authenticateToken, async (req, res) => {
  try {
    const {
      avatar,
      coverPhoto,
      displayName,
      bio,
      aboutMe,
      customStatus,
      story,
      stories,
      note,
      profileTheme,
      chatBackground,
      privacy
    } = req.body;

    await cleanupExpiredEphemeralContent();

    const allowedThemes = new Set(['neon-pink', 'purple', 'blue', 'green', 'dark']);
    const allowedChatBackgrounds = ALLOWED_CHAT_THEMES;
    const profileData = {};

    if (avatar) profileData.avatar = avatar;
    if (coverPhoto !== undefined) profileData.coverPhoto = coverPhoto || '';
    if (displayName !== undefined) {
      profileData.displayName = String(displayName).trim().slice(0, 40) || req.user.username;
    }
    if (bio !== undefined) profileData.bio = String(bio).slice(0, 180) || 'Infinity Chat user';
    if (aboutMe !== undefined) profileData.aboutMe = String(aboutMe).slice(0, 500);
    if (customStatus !== undefined) profileData.customStatus = String(customStatus).slice(0, 80);
    if (story !== undefined) {
      if (story && story.content) {
        const storyType = story.type === 'image' ? 'image' : 'text';

        profileData.story = {
          type: storyType,
          content: String(story.content).slice(0, storyType === 'image' ? 900000 : 280),
          createdAt: story.createdAt ? new Date(story.createdAt) : new Date(),
          views: Array.isArray(story.views) ? story.views : [],
          reactions: Array.isArray(story.reactions) ? story.reactions : []
        };
      } else {
        profileData.story = null;
      }
    }
    if (stories !== undefined && Array.isArray(stories)) {
      profileData.stories = stories
        .map(sanitizeStoryItem)
        .filter(Boolean)
        .slice(-20);
    }
    if (note !== undefined) {
      profileData.note = note?.text
        ? {
            text: String(note.text).slice(0, 60),
            updatedAt: note.updatedAt ? new Date(note.updatedAt) : new Date()
          }
        : null;
    }
    if (allowedThemes.has(profileTheme)) profileData.profileTheme = profileTheme;
    if (allowedChatBackgrounds.has(chatBackground)) profileData.chatBackground = chatBackground;
    if (privacy && typeof privacy === 'object') {
      profileData.privacy = {
        showOnlineStatus: privacy.showOnlineStatus !== false,
        showLastSeen: privacy.showLastSeen !== false,
        allowDirectMessages: privacy.allowDirectMessages !== false,
        allowProfileViewing: privacy.allowProfileViewing !== false
      };
    }

    let user;

    if (isMongoConnected) {
      user = await User.findById(req.user.id);

      if (!user) {
        return res.status(404).json({
          error: 'User not found.'
        });
      }

      Object.assign(user, profileData);

      await user.save();
    } else {
      user = await localDbMock.updateUserProfile(
        req.user.id,
        profileData
      );

      if (!user) {
        return res.status(404).json({
          error: 'User not found.'
        });
      }
    }

    if (cleanUserExpiringContent(user)) {
      if (isMongoConnected) {
        user.markModified('story');
        user.markModified('stories');
        user.markModified('note');
        await user.save();
      } else {
        saveLocalDbData();
      }
    }

    const token = createUserToken(user);

    res.cookie('token', token, {
      httpOnly: true,
      secure: process.env.NODE_ENV === 'production',
      maxAge: 7 * 24 * 60 * 60 * 1000
    });

    let [updatedUser] = await addMessageCountsToResponses([userResponse(user)]);

    const profilePayload = JSON.stringify({
      type: 'profile_updated',
      user: updatedUser
    });

    wss.clients.forEach(client => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(profilePayload);
      }
    });

    res.json({
      success: true,
      token,
      user: updatedUser
    });
  } catch (err) {
    console.error('[PROFILE UPDATE ERROR]', err);

    res.status(500).json({
      error: 'Failed to update profile.'
    });
  }
});

// ================================
// USERS ROUTE
// ================================
app.get('/api/users', authenticateToken, async (req, res) => {
  try {
    await cleanupExpiredEphemeralContent();

    let usersList = [];

    if (isMongoConnected) {
      const dbUsers = await User.find(
        {},
        'username displayName avatar coverPhoto bio aboutMe customStatus story stories note profileTheme chatBackground privacy lastSeen createdAt'
      ).lean();

      usersList = dbUsers.map(userResponse);
    } else {
      usersList = (await localDbMock.getAllUsers()).map(userResponse);
    }

    usersList = await addMessageCountsToResponses(usersList);

    res.status(200).json({
      success: true,
      users: usersList
    });
  } catch (error) {
    console.error('[GET USERS ERROR]', error);

    res.status(500).json({
      error: 'Failed to retrieve users.'
    });
  }
});

app.get('/api/rooms/:roomId/theme', authenticateToken, async (req, res) => {
  try {
    const roomId = normalizeDmRoomId(req.params.roomId);

    if (!roomId || !canAccessDmRoom(roomId, req.user.username)) {
      return res.status(403).json({ error: 'Access denied: Invalid direct message room.' });
    }

    res.json({
      success: true,
      roomId,
      chatTheme: await getRoomChatTheme(roomId)
    });
  } catch (error) {
    console.error('[GET ROOM THEME ERROR]', error);
    res.status(500).json({ error: 'Failed to retrieve chat theme.' });
  }
});

app.put('/api/rooms/:roomId/theme', authenticateToken, async (req, res) => {
  try {
    const roomId = normalizeDmRoomId(req.params.roomId);
    const chatTheme = normalizeChatTheme(req.body?.chatTheme);

    if (!roomId || !canAccessDmRoom(roomId, req.user.username)) {
      return res.status(403).json({ error: 'Access denied: Invalid direct message room.' });
    }

    if (!CHAT_THEME_IDS.includes(chatTheme)) {
      return res.status(400).json({ error: 'Invalid chat theme.' });
    }

    const setting = await saveRoomChatTheme(roomId, chatTheme, req.user.username);
    broadcastRoomThemeUpdated(setting);

    res.json({
      success: true,
      roomId,
      chatTheme: setting.chatTheme
    });
  } catch (error) {
    console.error('[SAVE ROOM THEME ERROR]', error);
    res.status(500).json({ error: 'Failed to save chat theme.' });
  }
});

app.get('/api/stories', authenticateToken, async (req, res) => {
  try {
    await cleanupExpiredEphemeralContent();

    let usersList = [];

    if (isMongoConnected) {
      const dbUsers = await User.find(
        {
          $or: [
            { 'story.content': { $exists: true, $ne: '' } },
            { 'stories.0': { $exists: true } },
            { 'note.text': { $exists: true, $ne: '' } }
          ]
        },
        'username displayName avatar coverPhoto story stories note'
      ).lean();

      usersList = dbUsers.map(userResponse);
    } else {
      usersList = (await localDbMock.getAllUsers()).map(userResponse);
    }

    const profiles = usersList.map(user => {
      const stories = getActiveStories(user);

      return {
        username: user.username,
        displayName: user.displayName,
        avatar: user.avatar,
        coverPhoto: user.coverPhoto || '',
        story: stories[0] || null,
        stories,
        note: user.note || null
      };
    }).filter(user => user.stories?.length || user.story || user.note);

    res.json({
      success: true,
      profiles
    });
  } catch (error) {
    console.error('[GET STORIES ERROR]', error);

    res.status(500).json({
      error: 'Failed to retrieve stories.'
    });
  }
});

app.post('/api/stories', authenticateToken, async (req, res) => {
  try {
    const story = sanitizeStoryItem(req.body?.story || req.body);

    if (!story) {
      return res.status(400).json({ error: 'Story content is required.' });
    }

    let user;

    if (isMongoConnected) {
      user = await User.findById(req.user.id);
    } else {
      user = localDbMock.users.find(u => u._id === req.user.id);
    }

    if (!user) return res.status(404).json({ error: 'User not found.' });

    user.stories = getActiveStories(user)
      .filter(item => item.id !== 'legacy_story')
      .concat(story)
      .slice(-20);

    if (isMongoConnected) {
      user.markModified('stories');
      await user.save();
    } else {
      saveLocalDbData();
    }

    const [updatedUser] = await addMessageCountsToResponses([userResponse(user)]);
    broadcastStoryListUpdated(updatedUser.username, updatedUser.stories);
    broadcastProfileUpdated(updatedUser);

    res.json({ success: true, user: updatedUser });
  } catch (error) {
    console.error('[ADD STORY ERROR]', error);
    res.status(500).json({ error: 'Failed to save story.' });
  }
});

app.delete('/api/stories/:id', authenticateToken, async (req, res) => {
  try {
    const storyId = req.params.id;
    let user;

    if (isMongoConnected) {
      user = await User.findById(req.user.id);
    } else {
      user = localDbMock.users.find(u => u._id === req.user.id);
    }

    if (!user) return res.status(404).json({ error: 'User not found.' });

    user.stories = Array.isArray(user.stories)
      ? user.stories.filter(story => story.id !== storyId)
      : [];

    if (storyId === 'legacy_story') user.story = null;

    if (isMongoConnected) {
      user.markModified('stories');
      await user.save();
    } else {
      saveLocalDbData();
    }

    const [updatedUser] = await addMessageCountsToResponses([userResponse(user)]);
    broadcastStoryListUpdated(updatedUser.username, updatedUser.stories);
    broadcastProfileUpdated(updatedUser);

    res.json({ success: true, user: updatedUser });
  } catch (error) {
    console.error('[DELETE STORY ERROR]', error);
    res.status(500).json({ error: 'Failed to delete story.' });
  }
});

app.post('/api/stories/:username/view', authenticateToken, async (req, res) => {
  try {
    const ownerUsername = req.params.username;
    const storyId = req.body?.storyId;

    if (!ownerUsername || ownerUsername === req.user.username) {
      return res.json({ success: true });
    }

    let owner;
    let viewer;

    if (isMongoConnected) {
      owner = await User.findOne({ username: ownerUsername.toLowerCase() });
      viewer = await User.findById(req.user.id);
    } else {
      owner = await localDbMock.findUser(ownerUsername);
      viewer = localDbMock.users.find(u => u._id === req.user.id);
    }

    const story = getStoryTarget(owner, storyId);

    if (!story) {
      return res.status(404).json({ error: 'Story not found.' });
    }

    const views = story.views || [];
    const existingView = views.find(view => view.username === req.user.username);

    if (existingView) {
      existingView.viewedAt = new Date();
      existingView.avatar = viewer?.avatar || existingView.avatar || '';
    } else {
      views.push({
        username: req.user.username,
        avatar: viewer?.avatar || '',
        viewedAt: new Date()
      });
    }

    story.views = views;
    const currentView = views.find(view => view.username === req.user.username);

    broadcastStoryViewUpdated(ownerUsername.toLowerCase(), story.id || storyId, {
      username: currentView.username,
      avatar: currentView.avatar || '',
      viewedAt: currentView.viewedAt
    });

    if (isMongoConnected) {
      owner.markModified('stories');
      owner.markModified('story');
      await owner.save();
    } else {
      saveLocalDbData();
    }

    const [updatedUser] = await addMessageCountsToResponses([userResponse(owner)]);
    broadcastProfileUpdated(updatedUser);

    res.json({
      success: true,
      user: updatedUser
    });
  } catch (error) {
    console.error('[STORY VIEW ERROR]', error);
    res.status(500).json({ error: 'Failed to record story view.' });
  }
});

app.post('/api/stories/:username/reaction', authenticateToken, async (req, res) => {
  try {
    const ownerUsername = req.params.username;
    const storyId = req.body?.storyId;
    const reactionType = String(req.body?.type || '').slice(0, 12);

    if (!ownerUsername || ownerUsername === req.user.username || !reactionType) {
      return res.json({ success: true });
    }

    const updatedUser = await applyStoryReaction(
      ownerUsername,
      req.user.username,
      req.user.id,
      storyId,
      reactionType
    );
    const [updatedUserWithCounts] = await addMessageCountsToResponses([updatedUser]);

    broadcastProfileUpdated(updatedUserWithCounts);

    res.json({
      success: true,
      user: updatedUserWithCounts
    });
  } catch (error) {
    console.error('[STORY REACTION ERROR]', error);
    res.status(error.statusCode || 500).json({ error: error.message || 'Failed to record story reaction.' });
  }
});

// ================================
// GROUP ROUTES
// ================================
app.get('/api/groups', authenticateToken, async (req, res) => {
  try {
    let groupsList = [];

    if (isMongoConnected) {
      groupsList = await Group.find({
        members: req.user.username
      })
        .select('name creator members avatarThumb createdAt')
        .lean();
    } else {
      groupsList = await localDbMock.getGroupsForUser(req.user.username);
      groupsList = groupsList.map(group => ({
        _id: group._id,
        name: group.name,
        creator: group.creator,
        members: group.members,
        avatarThumb: group.avatarThumb || '',
        createdAt: group.createdAt
      }));
    }

    res.status(200).json({
      success: true,
      groups: groupsList
    });
  } catch (error) {
    console.error('[GET GROUPS ERROR]', error);

    res.status(500).json({
      error: 'Failed to retrieve groups.'
    });
  }
});

app.post('/api/groups', authenticateToken, async (req, res) => {
  try {
    const { name, members } = req.body;

    if (!name || !name.trim()) {
      return res.status(400).json({
        error: 'Group name is required.'
      });
    }

    if (!members || members.length === 0) {
      return res.status(400).json({
        error: 'At least one member must be selected to create a group.'
      });
    }

    const creator = req.user.username;

    const finalMembers = Array.from(
      new Set([creator, ...members])
    );

    const groupAvatar =
      `https://api.dicebear.com/7.x/initials/svg?seed=${encodeURIComponent(
        name.trim()
      )}&backgroundColor=ff2da6,c026d3&fontColor=ffffff`;

    let newGroup;

    if (isMongoConnected) {
      newGroup = new Group({
        name: name.trim(),
        creator,
        members: finalMembers,
        avatar: groupAvatar,
        avatarThumb: groupAvatar
      });

      await newGroup.save();
    } else {
      newGroup = await localDbMock.createGroup(
        name.trim(),
        creator,
        finalMembers,
        groupAvatar
      );
      newGroup.avatarThumb = groupAvatar;
      saveLocalDbData();
    }

    const groupCreatedPayload = JSON.stringify({
      type: 'group_created',
      group: toGroupListItem(newGroup)
    });

    finalMembers.forEach(memberUsername => {
      const socketSet = userSockets.get(memberUsername);

      if (socketSet) {
        socketSet.forEach(s => {
          if (s.readyState === WebSocket.OPEN) {
            s.send(groupCreatedPayload);
          }
        });
      }
    });

    res.status(201).json({
      success: true,
      group: toGroupListItem(newGroup)
    });
  } catch (error) {
    console.error('[CREATE GROUP ERROR]', error);

    res.status(500).json({
      error: 'Failed to create group.'
    });
  }
});

app.post('/api/groups/:id/leave', authenticateToken, async (req, res) => {
  try {
    const groupId = req.params.id;
    const username = req.user.username;

    let group;

    if (isMongoConnected) {
      group = await Group.findById(groupId);

      if (group) {
        if (group.creator === username) {
          return res.status(400).json({
            error: 'Group creator cannot leave.'
          });
        }

        group.members = group.members.filter(m => m !== username);

        await group.save();
      }
    } else {
      const tempGroup = localDbMock.groups.find(g => g._id === groupId);

      if (tempGroup && tempGroup.creator === username) {
        return res.status(400).json({
          error: 'Group creator cannot leave.'
        });
      }

      group = await localDbMock.leaveGroup(groupId, username);
    }

    if (!group) {
      return res.status(404).json({
        error: 'Group not found.'
      });
    }

    const leavePayload = JSON.stringify({
      type: 'group_updated',
      group
    });

    group.members.forEach(member => {
      const socketSet = userSockets.get(member);

      if (socketSet) {
        socketSet.forEach(s => {
          if (s.readyState === WebSocket.OPEN) {
            s.send(leavePayload);
          }
        });
      }
    });

    res.status(200).json({
      success: true,
      group
    });
  } catch (err) {
    console.error('[LEAVE GROUP ERROR]', err);

    res.status(500).json({
      error: 'Failed to leave group.'
    });
  }
});

app.put('/api/groups/:id', authenticateToken, async (req, res) => {
  try {
    const groupId = req.params.id;
    const username = req.user.username;
    const name = String(req.body.name || '').trim().slice(0, 40);
    const avatar = String(req.body.avatar || '').trim();
    const avatarThumb = String(req.body.avatarThumb || '').trim();

    if (!name && !avatar && !avatarThumb) {
      return res.status(400).json({
        error: 'Group name or photo is required.'
      });
    }

    let group;

    if (isMongoConnected) {
      group = await Group.findById(groupId);

      if (group) {
        if (!(group.members || []).includes(username)) {
          return res.status(403).json({
            error: 'Only group members can change group info.'
          });
        }

        if (name) group.name = name;
        if (avatar) group.avatar = avatar;
        if (avatarThumb) group.avatarThumb = avatarThumb;
        await group.save();
      }
    } else {
      const tempGroup = localDbMock.groups.find(g => g._id === groupId);

      if (tempGroup && !(tempGroup.members || []).includes(username)) {
        return res.status(403).json({
          error: 'Only group members can change group info.'
        });
      }

      group = await localDbMock.updateGroupInfo(groupId, { name, avatar, avatarThumb });
    }

    if (!group) {
      return res.status(404).json({
        error: 'Group not found.'
      });
    }

    const updatePayload = JSON.stringify({
      type: 'group_updated',
      group: toGroupListItem(group)
    });

    group.members.forEach(member => {
      const socketSet = userSockets.get(member);

      if (socketSet) {
        socketSet.forEach(s => {
          if (s.readyState === WebSocket.OPEN) {
            s.send(updatePayload);
          }
        });
      }
    });

    res.status(200).json({
      success: true,
      group: toGroupListItem(group)
    });
  } catch (err) {
    console.error('[UPDATE GROUP ERROR]', err);

    res.status(500).json({
      error: 'Failed to update group.'
    });
  }
});

app.delete('/api/groups/:id/conversation', authenticateToken, async (req, res) => {
  try {
    const groupId = req.params.id;
    const username = req.user.username;
    const roomId = `group_${groupId}`;
    let group;

    if (isMongoConnected) {
      group = await Group.findById(groupId);
    } else {
      group = localDbMock.groups.find(g => g._id === groupId);
    }

    if (!group) {
      return res.status(404).json({
        error: 'Group not found.'
      });
    }

    if (!(group.members || []).includes(username)) {
      return res.status(403).json({
        error: 'You are not a member of this group.'
      });
    }

    if (isMongoConnected) {
      await Message.deleteMany({ roomId });
      await ReadReceipt.deleteMany({ roomId });
    } else {
      await localDbMock.clearRoomMessages(roomId);
    }

    try {
      const cacheClient = getRedisClient();
      await cacheClient.del(`chat_history:${roomId}`);
    } catch (cacheErr) {
      console.warn('[CLEAR GROUP CACHE ERROR]', cacheErr.message);
    }

    await routePayloadToRoom(roomId, {
      type: 'conversation_cleared',
      roomId
    });

    res.status(200).json({
      success: true
    });
  } catch (err) {
    console.error('[DELETE GROUP CONVERSATION ERROR]', err);

    res.status(500).json({
      error: 'Failed to delete conversation.'
    });
  }
});

app.post('/api/reports', authenticateToken, async (req, res) => {
  try {
    const report = {
      username: req.user.username,
      roomId: String(req.body.roomId || ''),
      category: String(req.body.category || 'technical_problem').slice(0, 80),
      message: String(req.body.message || '').slice(0, 1000)
    };

    if (!report.message.trim()) {
      return res.status(400).json({
        error: 'Report message is required.'
      });
    }

    await localDbMock.saveReport(report);

    res.status(200).json({
      success: true
    });
  } catch (err) {
    console.error('[REPORT ERROR]', err);

    res.status(500).json({
      error: 'Failed to submit report.'
    });
  }
});

app.put('/api/groups/:id/members', authenticateToken, async (req, res) => {
  try {
    const groupId = req.params.id;
    const { members } = req.body;
    const username = req.user.username;
    let previousMembers = [];

    if (!members || members.length === 0) {
      return res.status(400).json({
        error: 'Group must have at least one member.'
      });
    }

    let group;

    if (isMongoConnected) {
      group = await Group.findById(groupId);

      if (group) {
        previousMembers = [...group.members];

        if (group.creator !== username) {
          return res.status(403).json({
            error: 'Only the creator can manage members.'
          });
        }

        group.members = Array.from(
          new Set([group.creator, ...members])
        );

        await group.save();
      }
    } else {
      const tempGroup = localDbMock.groups.find(g => g._id === groupId);

      if (tempGroup && tempGroup.creator !== username) {
        return res.status(403).json({
          error: 'Only the creator can manage members.'
        });
      }

      previousMembers = tempGroup ? [...tempGroup.members] : [];
      group = await localDbMock.updateGroupMembers(groupId, members);
    }

    if (!group) {
      return res.status(404).json({
        error: 'Group not found.'
      });
    }

    const updatePayload = JSON.stringify({
      type: 'group_updated',
      group
    });

    Array.from(new Set([...previousMembers, ...group.members])).forEach(member => {
      const socketSet = userSockets.get(member);

      if (socketSet) {
        socketSet.forEach(s => {
          if (s.readyState === WebSocket.OPEN) {
            s.send(updatePayload);
          }
        });
      }
    });

    res.status(200).json({
      success: true,
      group
    });
  } catch (err) {
    console.error('[UPDATE MEMBERS ERROR]', err);

    res.status(500).json({
      error: 'Failed to update members.'
    });
  }
});

// ================================
// WEBSOCKET SERVER
// ================================
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const userSockets = new Map();

wss.on('error', err => {
  if (err.code === 'EADDRINUSE') {
    console.error(
      `[SYSTEM] WebSocket port ${PORT} is already in use. Run npm run restart, or stop the old server first.`
    );
    process.exit(1);
  }

  console.error('[WEBSOCKET SERVER ERROR]', err);
});

async function getUserAvatar(username) {
  const normalizedUsername = String(username || '').toLowerCase();

  if (!normalizedUsername) return '';

  if (avatarCache.has(normalizedUsername)) {
    return avatarCache.get(normalizedUsername) || '';
  }

  try {
    if (isMongoConnected) {
      const user = await User.findOne(
        { username: normalizedUsername },
        'avatar username'
      ).lean();
      const avatar = user?.avatar || '';

      avatarCache.set(normalizedUsername, avatar);
      return avatar;
    }

    const user = await localDbMock.findUser(username);
    const avatar = user?.avatar || '';

    avatarCache.set(normalizedUsername, avatar);
    return avatar;
  } catch {
    return '';
  }
}

async function getUserAvatars(usernames) {
  const normalizedNames = Array.from(
    new Set(
      usernames
        .map(username => String(username || '').toLowerCase())
        .filter(Boolean)
    )
  );
  const avatars = new Map();
  const missingNames = [];

  normalizedNames.forEach(username => {
    if (avatarCache.has(username)) {
      avatars.set(username, avatarCache.get(username) || '');
    } else {
      missingNames.push(username);
    }
  });

  if (!missingNames.length) return avatars;

  try {
    if (isMongoConnected) {
      const users = await User.find(
        { username: { $in: missingNames } },
        'username avatar'
      ).lean();

      users.forEach(user => {
        const normalizedUsername = String(user.username || '').toLowerCase();
        const avatar = user.avatar || '';

        avatarCache.set(normalizedUsername, avatar);
        avatars.set(normalizedUsername, avatar);
      });
    } else {
      missingNames.forEach(username => {
        const user = localDbMock.users.find(u => u.username === username);
        const avatar = user?.avatar || '';

        avatarCache.set(username, avatar);
        avatars.set(username, avatar);
      });
    }
  } catch {
    missingNames.forEach(username => avatars.set(username, ''));
  }

  return avatars;
}

async function getGroupMembersByRoom(roomId) {
  const groupId = roomId.replace('group_', '');

  if (isMongoConnected) {
    if (!mongoose.Types.ObjectId.isValid(groupId)) return [];

    const group = await Group.findById(groupId);
    return group ? group.members : [];
  }

  const group = localDbMock.groups.find(g => g._id === groupId);
  return group ? group.members : [];
}

function getDmMembers(roomId) {
  return roomId.replace('dm_', '').split('_');
}

function getDmRecipient(roomId, senderUsername) {
  return getDmMembers(roomId).find(
    member => member.toLowerCase() !== String(senderUsername || '').toLowerCase()
  );
}

function normalizeDmRoomId(roomId) {
  if (!String(roomId || '').startsWith('dm_')) return '';

  const members = getDmMembers(roomId)
    .map(member => String(member || '').trim().toLowerCase())
    .filter(Boolean)
    .sort();

  if (members.length !== 2 || members[0] === members[1]) return '';
  return `dm_${members.join('_')}`;
}

function canAccessDmRoom(roomId, username) {
  const normalizedRoomId = normalizeDmRoomId(roomId);
  if (!normalizedRoomId) return false;

  return getDmMembers(normalizedRoomId).includes(String(username || '').toLowerCase());
}

async function getRoomChatTheme(roomId) {
  const normalizedRoomId = normalizeDmRoomId(roomId);
  if (!normalizedRoomId) return 'pink-dream';

  if (isMongoConnected) {
    const setting = await RoomSetting.findOne({ roomId: normalizedRoomId }).lean();
    return normalizeChatTheme(setting?.chatTheme);
  }

  return normalizeChatTheme(localDbMock.roomSettings?.[normalizedRoomId]?.chatTheme);
}

async function saveRoomChatTheme(roomId, theme, username) {
  const normalizedRoomId = normalizeDmRoomId(roomId);
  const chatTheme = normalizeChatTheme(theme);
  const members = getDmMembers(normalizedRoomId);
  const payload = {
    roomId: normalizedRoomId,
    type: 'dm',
    members,
    chatTheme,
    updatedBy: username,
    updatedAt: new Date()
  };

  if (isMongoConnected) {
    await RoomSetting.findOneAndUpdate(
      { roomId: normalizedRoomId },
      payload,
      { upsert: true, new: true, setDefaultsOnInsert: true }
    );
  } else {
    localDbMock.roomSettings[normalizedRoomId] = payload;
    saveLocalDbData();
  }

  return payload;
}

function broadcastRoomThemeUpdated(setting) {
  const payload = JSON.stringify({
    type: 'room_theme_updated',
    roomId: setting.roomId,
    chatTheme: setting.chatTheme,
    updatedBy: setting.updatedBy,
    updatedAt: setting.updatedAt
  });

  wss.clients.forEach(client => {
    if (
      client.readyState === WebSocket.OPEN &&
      setting.members.includes(String(client.username || '').toLowerCase())
    ) {
      client.send(payload);
    }
  });
}

async function getUserByUsername(username) {
  if (!username) return null;

  if (isMongoConnected) {
    return await User.findOne({ username: String(username).toLowerCase() });
  }

  return await localDbMock.findUser(username);
}

async function canUseDirectMessageRoom(roomId, senderUsername) {
  const members = getDmMembers(roomId);

  if (!members.includes(senderUsername)) {
    return {
      allowed: false,
      message: 'Access denied: Invalid direct message room.'
    };
  }

  const recipientUsername = getDmRecipient(roomId, senderUsername);
  const recipient = await getUserByUsername(recipientUsername);

  if (recipient?.privacy?.allowDirectMessages === false) {
    return {
      allowed: false,
      message: `${recipientUsername} is not accepting direct messages.`
    };
  }

  return { allowed: true };
}

async function sendToUser(username, payload) {
  const socketSet = userSockets.get(username);

  if (!socketSet) return;

  socketSet.forEach(socket => {
    if (socket.readyState === WebSocket.OPEN) {
      socket.send(JSON.stringify(payload));
    }
  });
}

async function broadcastOnlineUsers() {
  try {
    const client = getRedisClient();
    const onlineUsers = await client.smembers('online_users');
    const avatars = await getUserAvatars(onlineUsers);

    const onlineUserList = onlineUsers.map(username => {
      const avatar = avatars.get(String(username || '').toLowerCase()) || '';

      return {
        username,
        avatar:
          avatar ||
          `https://api.dicebear.com/7.x/initials/svg?seed=${encodeURIComponent(
            username
          )}&backgroundColor=ff2da6,c026d3&fontColor=ffffff`
      };
    });

    const payload = JSON.stringify({
      type: 'online_list',
      users: onlineUserList,
      count: onlineUserList.length
    });

    wss.clients.forEach(clientSocket => {
      if (clientSocket.readyState === WebSocket.OPEN) {
        clientSocket.send(payload);
      }
    });
  } catch (err) {
    console.error('[PRESENCE BROADCAST ERROR]', err);
  }
}

async function getHistory(roomId, limit = 50, viewerUsername = '') {
  if (isMongoConnected) {
    const dbMsgs = await Message.find({ roomId })
      .sort({ timestamp: -1 })
      .limit(limit);

    return dbMsgs.reverse().map(m => ({
      _id: m._id,
      type: 'chat',
      roomId: m.roomId,
      username: m.sender,
      sender: m.sender,
      message: m.content,
      content: m.content,
      replyTo: m.replyTo?.messageId ? m.replyTo : null,
      reactions: Object.fromEntries(m.reactions || []),
      deletedForEveryone: Boolean(m.deletedForEveryone),
      deletedForMe: (m.deletedFor || []).includes(viewerUsername),
      timestamp: m.timestamp,
      status: m.status || 'sent',
      avatar: ''
    }));
  }

  const messages = await localDbMock.getRecentMessages(roomId, limit);

  return messages.map(message => ({
    ...message,
    deletedForEveryone: Boolean(message.deletedForEveryone),
    deletedForMe: (message.deletedFor || []).includes(viewerUsername)
  }));
}

async function getRoomReadReceipts(roomId) {
  if (isMongoConnected) {
    const receipts = await ReadReceipt.find({ roomId }).lean();
    return receipts.map(receipt => ({
      roomId: receipt.roomId,
      username: receipt.username,
      avatar: receipt.avatar || '',
      lastMessageId: receipt.lastMessageId,
      updatedAt: receipt.updatedAt
    }));
  }

  return Object.values(localDbMock.readReceipts?.[roomId] || {});
}

async function saveReadReceipt(roomId, username, avatar, lastMessageId) {
  const receipt = {
    roomId,
    username,
    avatar: avatar || '',
    lastMessageId: String(lastMessageId),
    updatedAt: new Date()
  };

  if (isMongoConnected) {
    await ReadReceipt.findOneAndUpdate(
      { roomId, username },
      receipt,
      { upsert: true, returnDocument: 'after', setDefaultsOnInsert: true }
    );
    return receipt;
  }

  if (!localDbMock.readReceipts) localDbMock.readReceipts = {};
  if (!localDbMock.readReceipts[roomId]) localDbMock.readReceipts[roomId] = {};
  localDbMock.readReceipts[roomId][username] = receipt;
  saveLocalDbData();
  return receipt;
}

function sanitizeReplyTo(replyTo) {
  if (!replyTo || typeof replyTo !== 'object') return null;

  const messageId = String(replyTo.messageId || replyTo._id || replyTo.id || '').trim();
  const username = String(replyTo.username || replyTo.sender || '').trim();
  const preview = String(replyTo.content || replyTo.message || '').trim();
  const roomId = String(replyTo.roomId || '').trim();

  if (!messageId || !username || !preview) return null;

  return {
    messageId: messageId.slice(0, 80),
    username: username.slice(0, 40),
    content: preview.slice(0, 140),
    roomId: roomId.slice(0, 100)
  };
}

async function saveChatMessage(username, content, roomId, replyTo = null) {
  const cleanReplyTo = sanitizeReplyTo(replyTo);

  if (isMongoConnected) {
    const message = new Message({
      sender: username,
      content,
      roomId,
      replyTo: cleanReplyTo,
      status: 'sent'
    });

    await message.save();

    return {
      _id: message._id,
      type: 'chat',
      roomId,
      username,
      sender: username,
      avatar: await getUserAvatar(username),
      message: content,
      content,
      replyTo: cleanReplyTo,
      reactions: {},
      deletedFor: [],
      deletedForEveryone: false,
      timestamp: message.timestamp,
      status: 'sent'
    };
  }

  return await localDbMock.saveMessage(
    username,
    content,
    'chat',
    roomId,
    cleanReplyTo
  );
}

async function setMessageReaction(messageId, username, emoji) {
  if (isMongoConnected) {
    const message = await Message.findById(messageId);

    if (!message) return null;

    const reactions = Object.fromEntries(message.reactions || []);

    Object.keys(reactions).forEach(key => {
      reactions[key] = reactions[key].filter(member => member !== username);

      if (reactions[key].length === 0) {
        delete reactions[key];
      }
    });

    if (emoji) {
      if (!reactions[emoji]) reactions[emoji] = [];
      reactions[emoji].push(username);
    }

    message.reactions = reactions;
    await message.save();

    return {
      _id: message._id,
      roomId: message.roomId,
      reactions
    };
  }

  const message = await localDbMock.setMessageReaction(
    messageId,
    username,
    emoji
  );

  return message
    ? {
        _id: message._id,
        roomId: message.roomId,
        reactions: message.reactions || {}
      }
    : null;
}

async function markMessageDeleted(messageId, username, mode) {
  if (isMongoConnected) {
    const message = await Message.findById(messageId);

    if (!message) return null;

    if (mode === 'everyone') {
      if (message.sender !== username) return null;

      message.deletedForEveryone = true;
      message.reactions = {};
    } else {
      message.deletedFor = Array.from(
        new Set([...(message.deletedFor || []), username])
      );
    }

    await message.save();

    return {
      _id: message._id,
      roomId: message.roomId,
      mode,
      username
    };
  }

  const message = localDbMock.messages.find(m => String(m._id) === String(messageId));

  if (!message) return null;

  if (mode === 'everyone') {
    if (message.sender !== username && message.username !== username) return null;

    message.deletedForEveryone = true;
    message.reactions = {};
  } else {
    message.deletedFor = Array.from(
      new Set([...(message.deletedFor || []), username])
    );
  }

  saveLocalDbData();

  return {
    _id: message._id,
    roomId: message.roomId,
    mode,
    username
  };
}

async function routePayloadToRoom(roomId, payload) {
  if (roomId === 'lounge') {
    wss.clients.forEach(clientSocket => {
      if (clientSocket.readyState === WebSocket.OPEN) {
        clientSocket.send(JSON.stringify(payload));
      }
    });

    return;
  }

  if (roomId.startsWith('dm_')) {
    const members = getDmMembers(roomId);

    members.forEach(username => {
      sendToUser(username, payload);
    });

    return;
  }

  if (roomId.startsWith('group_')) {
    const members = await getGroupMembersByRoom(roomId);

    members.forEach(username => {
      sendToUser(username, payload);
    });
  }
}

async function routeTypingToRoom(roomId, senderUsername) {
  const typingPayload = {
    type: 'typing',
    roomId,
    username: senderUsername
  };

  if (roomId === 'lounge') {
    wss.clients.forEach(clientSocket => {
      if (
        clientSocket.readyState === WebSocket.OPEN &&
        clientSocket.username !== senderUsername
      ) {
        clientSocket.send(JSON.stringify(typingPayload));
      }
    });

    return;
  }

  if (roomId.startsWith('dm_')) {
    const members = getDmMembers(roomId);

    members.forEach(username => {
      if (username !== senderUsername) {
        sendToUser(username, typingPayload);
      }
    });

    return;
  }

  if (roomId.startsWith('group_')) {
    const members = await getGroupMembersByRoom(roomId);

    members.forEach(username => {
      if (username !== senderUsername) {
        sendToUser(username, typingPayload);
      }
    });
  }
}

wss.on('connection', async (ws, req) => {
  const urlParams = new URLSearchParams(req.url.split('?')[1]);
  const token = urlParams.get('token');

  let decodedUser;

  try {
    if (!token) throw new Error('No authentication token provided.');
    decodedUser = jwt.verify(token, JWT_SECRET);
  } catch (err) {
    ws.send(
      JSON.stringify({
        type: 'system',
        message: 'Authentication failed. Closing connection.'
      })
    );

    ws.close(4001, 'Unauthorized');
    return;
  }

  const { username, avatar } = decodedUser;

  ws.username = username;
  ws.avatar = avatar;

  if (!userSockets.has(username)) {
    userSockets.set(username, new Set());
  }

  userSockets.get(username).add(ws);

  const client = getRedisClient();
  await client.sadd('online_users', username);

  ws.send(
    JSON.stringify({
      type: 'system',
      message: `Welcome to the secure chat, @${username}!`
    })
  );

  await broadcastOnlineUsers();

  ws.on('message', async rawData => {
    try {
      const parsedData = JSON.parse(rawData.toString());

      if (parsedData.type === 'typing') {
        const targetRoomId = parsedData.roomId || 'lounge';

        if (targetRoomId.startsWith('dm_')) {
          const dmAccess = await canUseDirectMessageRoom(targetRoomId, username);
          if (!dmAccess.allowed) return;
        }

        await routeTypingToRoom(targetRoomId, username);

        return;
      }

      if (parsedData.type === 'get_history') {
        const targetRoomId = parsedData.roomId || 'lounge';

        if (targetRoomId.startsWith('dm_')) {
          const dmAccess = await canUseDirectMessageRoom(targetRoomId, username);

          if (!dmAccess.allowed) {
            ws.send(
              JSON.stringify({
                type: 'history',
                roomId: targetRoomId,
                messages: []
              })
            );
            ws.send(
              JSON.stringify({
                type: 'system',
                message: dmAccess.message
              })
            );
            return;
          }
        }

        const history = await getHistory(targetRoomId, 50, username);
        const avatars = await getUserAvatars(
          history.map(msg => msg.username || msg.sender)
        );
        const historyWithAvatars = history.map(msg => ({
          ...msg,
          avatar:
            msg.avatar ||
            avatars.get(String(msg.username || msg.sender || '').toLowerCase()) ||
            ''
        }));
        const readReceipts = await getRoomReadReceipts(targetRoomId);

        ws.send(
          JSON.stringify({
            type: 'history',
            roomId: targetRoomId,
            messages: historyWithAvatars,
            readReceipts
          })
        );

        return;
      }

      if (parsedData.type === 'read_receipt') {
        const targetRoomId = parsedData.roomId || 'lounge';
        const lastMessageId = String(parsedData.lastMessageId || '');

        if (!lastMessageId) return;

        if (targetRoomId.startsWith('group_')) {
          const members = await getGroupMembersByRoom(targetRoomId);

          if (!members.includes(username)) return;
        }

        if (targetRoomId.startsWith('dm_')) {
          const dmAccess = await canUseDirectMessageRoom(targetRoomId, username);

          if (!dmAccess.allowed) return;
        }

        const receipt = await saveReadReceipt(
          targetRoomId,
          username,
          ws.avatar || '',
          lastMessageId
        );

        await routePayloadToRoom(targetRoomId, {
          type: 'read_receipt',
          roomId: targetRoomId,
          username: receipt.username,
          avatar: receipt.avatar,
          lastMessageId: receipt.lastMessageId,
          updatedAt: receipt.updatedAt
        });

        return;
      }

      if (parsedData.type === 'story_reaction') {
        const ownerUsername = String(parsedData.ownerUsername || '').trim().toLowerCase();
        const storyId = parsedData.storyId;
        const reactionType = String(parsedData.reactionType || '').slice(0, 12);

        if (!ownerUsername || ownerUsername === username || !reactionType) return;

        broadcastStoryReactionUpdated(ownerUsername, storyId, {
          username,
          avatar: ws.avatar || '',
          type: reactionType,
          reactedAt: new Date().toISOString()
        });

        const updatedUser = await applyStoryReaction(
          ownerUsername,
          username,
          decodedUser.id,
          storyId,
          reactionType
        );

        if (updatedUser) broadcastProfileUpdated(updatedUser);
        return;
      }

      if (parsedData.type === 'reaction') {
        const targetRoomId = parsedData.roomId || 'lounge';

        if (targetRoomId.startsWith('group_')) {
          const members = await getGroupMembersByRoom(targetRoomId);

          if (!members.includes(username)) return;
        }

        if (targetRoomId.startsWith('dm_')) {
          const dmAccess = await canUseDirectMessageRoom(targetRoomId, username);

          if (!dmAccess.allowed) return;
        }

        const updatedMessage = await setMessageReaction(
          parsedData.messageId,
          username,
          parsedData.emoji
        );

        if (!updatedMessage || updatedMessage.roomId !== targetRoomId) {
          return;
        }

        await routePayloadToRoom(targetRoomId, {
          type: 'reaction',
          roomId: targetRoomId,
          messageId: String(updatedMessage._id),
          reactions: updatedMessage.reactions
        });

        return;
      }

      if (parsedData.type === 'delete_message') {
        const targetRoomId = parsedData.roomId || 'lounge';
        const mode = parsedData.mode === 'everyone' ? 'everyone' : 'me';

        if (targetRoomId.startsWith('group_')) {
          const members = await getGroupMembersByRoom(targetRoomId);

          if (!members.includes(username)) return;
        }

        if (targetRoomId.startsWith('dm_')) {
          const dmAccess = await canUseDirectMessageRoom(targetRoomId, username);

          if (!dmAccess.allowed) return;
        }

        const deletedMessage = await markMessageDeleted(
          parsedData.messageId,
          username,
          mode
        );

        if (!deletedMessage || deletedMessage.roomId !== targetRoomId) {
          return;
        }

        const payload = {
          type: 'message_deleted',
          roomId: targetRoomId,
          messageId: String(deletedMessage._id),
          mode,
          username
        };

        if (mode === 'everyone') {
          await routePayloadToRoom(targetRoomId, payload);
        } else {
          sendToUser(username, payload);
        }

        return;
      }

      if (parsedData.type !== 'chat') return;

      const content = parsedData.message;

      if (!content || content.trim() === '') return;

      const targetRoomId = parsedData.roomId || 'lounge';

      if (targetRoomId.startsWith('group_')) {
        const members = await getGroupMembersByRoom(targetRoomId);

        if (!members.includes(username)) {
          ws.send(
            JSON.stringify({
              type: 'system',
              message: 'Access denied: You are not a member of this group.'
            })
          );

          return;
        }
      }

      if (targetRoomId.startsWith('dm_')) {
        const dmAccess = await canUseDirectMessageRoom(targetRoomId, username);

        if (!dmAccess.allowed) {
          ws.send(
            JSON.stringify({
              type: 'system',
              message: dmAccess.message
            })
          );

          return;
        }
      }

      const payload = await saveChatMessage(
        username,
        content.trim(),
        targetRoomId,
        parsedData.replyTo
      );

      if (parsedData.storyReply) {
        payload.storyReply = true;
      }

      const senderReceipt = await saveReadReceipt(
        targetRoomId,
        username,
        ws.avatar || payload.avatar || '',
        String(payload._id || payload.id || '')
      );

      const cacheKey = `chat_history:${targetRoomId}`;

      try {
        await client.rpush(cacheKey, payload);
        await client.ltrim(cacheKey, -50, -1);
      } catch (cacheErr) {
        console.warn('[CACHE MESSAGE ERROR]', cacheErr.message);
      }

      await routePayloadToRoom(targetRoomId, payload);
      await routePayloadToRoom(targetRoomId, {
        type: 'read_receipt',
        roomId: targetRoomId,
        username: senderReceipt.username,
        avatar: senderReceipt.avatar,
        lastMessageId: senderReceipt.lastMessageId,
        updatedAt: senderReceipt.updatedAt
      });
    } catch (error) {
      console.error('[WS ERROR]', error.message);
    }
  });

  ws.on('close', async () => {
    const socketSet = userSockets.get(username);

    if (socketSet) {
      socketSet.delete(ws);

      if (socketSet.size === 0) {
        userSockets.delete(username);

        await client.srem('online_users', username);

        if (isMongoConnected) {
          await User.findOneAndUpdate(
            { username: username.toLowerCase() },
            { lastSeen: new Date() }
          );
        } else {
          await localDbMock.updateLastSeen(username);
        }

        await broadcastOnlineUsers();
      }
    }
  });
});

// REST notification route
app.post('/api/notify', authenticateToken, async (req, res) => {
  const { notification } = req.body;

  if (!notification) {
    return res.status(400).json({
      error: "Missing 'notification' parameter in request body."
    });
  }

  console.log(`[HTTP REST] Notification broadcast: ${notification}`);

  const notificationPayload = JSON.stringify({
    type: 'notification',
    message: notification,
    timestamp: new Date()
  });

  let activeReceivers = 0;

  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(notificationPayload);
      activeReceivers++;
    }
  });

  res.status(200).json({
    success: true,
    message: `Notification pushed to ${activeReceivers} active client sessions.`
  });
});

app.get('/api/health', async (req, res) => {
  res.status(200).json({
    success: true,
    services: {
      mongodb: {
        configured: Boolean(mongoUri),
        connected: isMongoConnected,
        status: mongoStatus,
        database: mongoDbName,
        error: mongoLastError || null
      },
      redis: {
        configured: Boolean(redisUrl && redisToken),
        connected: isRedisConnected,
        status: redisStatus
      }
    },
    storage: isMongoConnected ? 'mongodb_atlas' : 'local-data.json',
    cache: isRedisConnected ? 'upstash_redis' : 'local_mock'
  });
});

// ================================
// START SERVER
// ================================
const PORT = process.env.PORT || 3000;

server.on('error', err => {
  if (err.code === 'EADDRINUSE') {
    console.error(
      `[SYSTEM] Port ${PORT} is already in use. Stop the other server first, or change PORT in .env.`
    );
    process.exit(1);
  }

  throw err;
});

let shutdownStarted = false;

async function shutdownServer(signal) {
  if (shutdownStarted) return;
  shutdownStarted = true;

  console.log(`[SYSTEM] ${signal} received. Shutting down...`);
  if (ephemeralCleanupTimer) clearInterval(ephemeralCleanupTimer);

  const forceExitTimer = setTimeout(() => {
    console.warn('[SYSTEM] Forced shutdown after timeout.');
    process.exit(1);
  }, 8000);
  forceExitTimer.unref?.();

  try {
    await new Promise(resolve => server.close(resolve));

    if (mongoose.connection.readyState !== 0) {
      await mongoose.connection.close(false);
    }

    console.log('[SYSTEM] Server stopped.');
    process.exit(0);
  } catch (err) {
    console.error('[SYSTEM] Error during shutdown.', err);
    process.exit(1);
  }
}

process.on('SIGINT', () => shutdownServer('Ctrl+C'));
process.on('SIGTERM', () => shutdownServer('SIGTERM'));

async function startServer() {
  console.log('[SYSTEM] Checking MongoDB Atlas and Upstash Redis...');
  await Promise.race([
    Promise.allSettled([mongoConnectPromise, redisConnectPromise]),
    new Promise(resolve => setTimeout(resolve, 20000))
  ]);

  server.listen(PORT, () => {
    console.log(`[SYSTEM] Server listening on http://localhost:${PORT}`);
    console.log(
      `[SYSTEM] Storage: ${isMongoConnected ? 'MongoDB Atlas' : 'local-data.json'}`
    );
    console.log(
      `[SYSTEM] Cache: ${isRedisConnected ? 'Upstash Redis' : 'local mock'}`
    );
  });

  Promise.allSettled([mongoConnectPromise, redisConnectPromise]).then(() => {
    cleanupExpiredEphemeralContent();
    ephemeralCleanupTimer = setInterval(cleanupExpiredEphemeralContent, EPHEMERAL_CLEANUP_INTERVAL_MS);
  });
}

startServer().catch(err => {
  console.error('[SYSTEM] Failed to start server.', err);
  process.exit(1);
});
