const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');
const bcrypt = require('bcryptjs');
const config = require('../config');
const logger = require('../utils/logger');

let db;

function getDb() {
  if (!db) {
    throw new Error('Database not initialized');
  }
  return db;
}

function initDb() {
  const dbPath = path.join(config.dataDir, 'plc_control.db');
  if (!fs.existsSync(config.dataDir)) {
    fs.mkdirSync(config.dataDir, { recursive: true });
  }

  db = new Database(dbPath);
  db.pragma('journal_mode = WAL');
  db.pragma('foreign_keys = ON');

  createTables();
  seedAdmin();
  seedDefaults();
  logger.info('Database initialized', { path: dbPath });
  return db;
}

function createTables() {
  db.exec(`
    CREATE TABLE IF NOT EXISTS users (
      id TEXT PRIMARY KEY,
      username TEXT UNIQUE NOT NULL,
      password_hash TEXT NOT NULL,
      display_name TEXT NOT NULL,
      role TEXT NOT NULL DEFAULT 'qc',
      avatar_url TEXT,
      is_active INTEGER NOT NULL DEFAULT 1,
      created_at TEXT NOT NULL DEFAULT (datetime('now') || 'Z'),
      updated_at TEXT NOT NULL DEFAULT (datetime('now') || 'Z')
    );

    CREATE TABLE IF NOT EXISTS folders (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      parent_id TEXT,
      type TEXT NOT NULL DEFAULT 'line',
      description TEXT,
      created_by TEXT NOT NULL,
      created_at TEXT NOT NULL DEFAULT (datetime('now') || 'Z'),
      updated_at TEXT NOT NULL DEFAULT (datetime('now') || 'Z'),
      is_deleted INTEGER NOT NULL DEFAULT 0,
      FOREIGN KEY (parent_id) REFERENCES folders(id),
      FOREIGN KEY (created_by) REFERENCES users(id)
    );

    CREATE INDEX IF NOT EXISTS idx_folders_parent_id ON folders(parent_id);
    CREATE UNIQUE INDEX IF NOT EXISTS idx_folders_name_parent
      ON folders(name, parent_id) WHERE is_deleted = 0;

    CREATE TABLE IF NOT EXISTS files (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      path TEXT NOT NULL,
      description TEXT,
      created_by TEXT NOT NULL,
      created_at TEXT NOT NULL DEFAULT (datetime('now') || 'Z'),
      updated_at TEXT NOT NULL DEFAULT (datetime('now') || 'Z'),
      is_deleted INTEGER NOT NULL DEFAULT 0,
      deleted_by TEXT,
      deleted_at TEXT,
      FOREIGN KEY (created_by) REFERENCES users(id),
      FOREIGN KEY (deleted_by) REFERENCES users(id)
    );

    CREATE UNIQUE INDEX IF NOT EXISTS idx_files_name_path
      ON files(name, path) WHERE is_deleted = 0;

    CREATE TABLE IF NOT EXISTS versions (
      id TEXT PRIMARY KEY,
      file_id TEXT NOT NULL,
      version_number INTEGER NOT NULL,
      storage_path TEXT NOT NULL,
      size INTEGER NOT NULL,
      checksum TEXT NOT NULL,
      mime_type TEXT,
      is_binary INTEGER NOT NULL DEFAULT 0,
      commit_message TEXT,
      uploaded_by TEXT NOT NULL,
      created_at TEXT NOT NULL DEFAULT (datetime('now') || 'Z'),
      FOREIGN KEY (file_id) REFERENCES files(id),
      FOREIGN KEY (uploaded_by) REFERENCES users(id)
    );

    CREATE INDEX IF NOT EXISTS idx_versions_file_id ON versions(file_id);

    CREATE TABLE IF NOT EXISTS activity_log (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      action TEXT NOT NULL,
      entity_type TEXT NOT NULL,
      entity_id TEXT,
      entity_name TEXT,
      details TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now') || 'Z'),
      FOREIGN KEY (user_id) REFERENCES users(id)
    );

    CREATE INDEX IF NOT EXISTS idx_activity_log_created_at ON activity_log(created_at);
    CREATE INDEX IF NOT EXISTS idx_activity_log_user_id ON activity_log(user_id);

    CREATE TABLE IF NOT EXISTS refresh_tokens (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      token_hash TEXT NOT NULL,
      expires_at TEXT NOT NULL,
      created_at TEXT NOT NULL DEFAULT (datetime('now') || 'Z'),
      FOREIGN KEY (user_id) REFERENCES users(id)
    );

    CREATE INDEX IF NOT EXISTS idx_refresh_tokens_user_id ON refresh_tokens(user_id);

    -- Feature 1: Tags
    CREATE TABLE IF NOT EXISTS tags (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL UNIQUE,
      color TEXT NOT NULL DEFAULT '#1677ff',
      created_by TEXT NOT NULL,
      created_at TEXT NOT NULL DEFAULT (datetime('now') || 'Z'),
      FOREIGN KEY (created_by) REFERENCES users(id)
    );

    CREATE TABLE IF NOT EXISTS file_tags (
      file_id TEXT NOT NULL,
      tag_id TEXT NOT NULL,
      PRIMARY KEY (file_id, tag_id),
      FOREIGN KEY (file_id) REFERENCES files(id),
      FOREIGN KEY (tag_id) REFERENCES tags(id)
    );

    -- Feature 2: Version comments
    CREATE TABLE IF NOT EXISTS version_comments (
      id TEXT PRIMARY KEY,
      version_id TEXT NOT NULL,
      user_id TEXT NOT NULL,
      content TEXT NOT NULL,
      created_at TEXT NOT NULL DEFAULT (datetime('now') || 'Z'),
      updated_at TEXT NOT NULL DEFAULT (datetime('now') || 'Z'),
      is_deleted INTEGER NOT NULL DEFAULT 0,
      FOREIGN KEY (version_id) REFERENCES versions(id),
      FOREIGN KEY (user_id) REFERENCES users(id)
    );

    CREATE INDEX IF NOT EXISTS idx_version_comments_version_id ON version_comments(version_id);

    -- Feature 3: Subscriptions & notifications
    CREATE TABLE IF NOT EXISTS file_subscriptions (
      id TEXT PRIMARY KEY,
      file_id TEXT NOT NULL,
      user_id TEXT NOT NULL,
      created_at TEXT NOT NULL DEFAULT (datetime('now') || 'Z'),
      UNIQUE(file_id, user_id),
      FOREIGN KEY (file_id) REFERENCES files(id),
      FOREIGN KEY (user_id) REFERENCES users(id)
    );

    CREATE TABLE IF NOT EXISTS notifications (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      type TEXT NOT NULL,
      title TEXT NOT NULL,
      message TEXT NOT NULL,
      entity_id TEXT,
      entity_type TEXT,
      is_read INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL DEFAULT (datetime('now') || 'Z'),
      FOREIGN KEY (user_id) REFERENCES users(id)
    );

    CREATE INDEX IF NOT EXISTS idx_notifications_user_id ON notifications(user_id);
    CREATE INDEX IF NOT EXISTS idx_file_subscriptions_file_id ON file_subscriptions(file_id);

    -- DM2000 dropdown options (Type / Manufacturer)
    CREATE TABLE IF NOT EXISTS dm2000_options (
      id TEXT PRIMARY KEY,
      field TEXT NOT NULL,
      value TEXT NOT NULL,
      created_by TEXT NOT NULL,
      created_at TEXT NOT NULL DEFAULT (datetime('now') || 'Z'),
      UNIQUE(field, value),
      FOREIGN KEY (created_by) REFERENCES users(id)
    );

    CREATE INDEX IF NOT EXISTS idx_dm2000_options_field ON dm2000_options(field);

    -- Battery types (loai pin) managed by admin
    CREATE TABLE IF NOT EXISTS battery_types (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL UNIQUE,
      created_by TEXT NOT NULL,
      created_at TEXT NOT NULL DEFAULT (datetime('now') || 'Z'),
      FOREIGN KEY (created_by) REFERENCES users(id)
    );

    -- Battery product lines (dong san pham) managed by admin
    CREATE TABLE IF NOT EXISTS battery_product_lines (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL UNIQUE,
      created_by TEXT NOT NULL,
      created_at TEXT NOT NULL DEFAULT (datetime('now') || 'Z'),
      FOREIGN KEY (created_by) REFERENCES users(id)
    );

    -- Battery presets (thong so) stored on server
    CREATE TABLE IF NOT EXISTS battery_presets (
      id TEXT PRIMARY KEY,
      battery_type TEXT NOT NULL,
      product_line TEXT NOT NULL,
      resistance REAL,
      ocv_time REAL,
      load_time REAL,
      k_coeff REAL,
      ocv_min REAL,
      ocv_max REAL,
      ccv_min REAL,
      ccv_max REAL,
      dia_min REAL,
      dia_max REAL,
      hei_min REAL,
      hei_max REAL,
      created_by TEXT NOT NULL,
      created_at TEXT NOT NULL DEFAULT (datetime('now') || 'Z'),
      updated_at TEXT NOT NULL DEFAULT (datetime('now') || 'Z'),
      UNIQUE(battery_type, product_line),
      FOREIGN KEY (created_by) REFERENCES users(id)
    );

    CREATE INDEX IF NOT EXISTS idx_battery_presets_type_line ON battery_presets(battery_type, product_line);

    -- Battery order history snapshots (lịch sử đơn hàng)
    CREATE TABLE IF NOT EXISTS battery_order_history (
      id TEXT PRIMARY KEY,
      order_id TEXT NOT NULL,
      test_date TEXT,
      battery_type TEXT,
      product_line TEXT,
      records_json TEXT NOT NULL DEFAULT '[]',
      chart_series_json TEXT NOT NULL DEFAULT '{}',
      readings_json TEXT NOT NULL DEFAULT '{}',
      saved_at TEXT NOT NULL DEFAULT (datetime('now') || 'Z'),
      created_by TEXT NOT NULL,
      FOREIGN KEY (created_by) REFERENCES users(id)
    );

    CREATE INDEX IF NOT EXISTS idx_battery_order_history_order_id ON battery_order_history(order_id);
    CREATE INDEX IF NOT EXISTS idx_battery_order_history_saved_at ON battery_order_history(saved_at);
    CREATE INDEX IF NOT EXISTS idx_battery_order_history_created_by ON battery_order_history(created_by);
  `);

  // Safe migration: add folder_id column to files if not present
  const fileColumns = db.prepare("PRAGMA table_info(files)").all();
  const hasFolderId = fileColumns.some(col => col.name === 'folder_id');
  if (!hasFolderId) {
    db.exec('ALTER TABLE files ADD COLUMN folder_id TEXT REFERENCES folders(id)');
  }

  // Safe migration: add lock columns to files
  const hasLockedBy = fileColumns.some(col => col.name === 'locked_by');
  if (!hasLockedBy) {
    db.exec('ALTER TABLE files ADD COLUMN locked_by TEXT DEFAULT NULL');
    db.exec('ALTER TABLE files ADD COLUMN locked_at TEXT DEFAULT NULL');
    db.exec('ALTER TABLE files ADD COLUMN lock_reason TEXT DEFAULT NULL');
  }

  // Safe migration: add dia/hei standard columns to battery_presets if not present
  const presetColumns = db.prepare("PRAGMA table_info(battery_presets)").all();
  const presetColNames = presetColumns.map(c => c.name);
  if (!presetColNames.includes('dia_min')) {
    db.exec('ALTER TABLE battery_presets ADD COLUMN dia_min REAL');
    db.exec('ALTER TABLE battery_presets ADD COLUMN dia_max REAL');
    db.exec('ALTER TABLE battery_presets ADD COLUMN hei_min REAL');
    db.exec('ALTER TABLE battery_presets ADD COLUMN hei_max REAL');
  }

}

function seedDefaults() {
  const { v4: uuidv4 } = require('uuid');
  const adminUser = db.prepare("SELECT id FROM users WHERE role = 'admin' LIMIT 1").get();
  const adminId = adminUser ? adminUser.id : null;
  if (!adminId) return; // should not happen — seedAdmin() runs first

  // Seed default battery types if table is empty
  const typeCount = db.prepare('SELECT COUNT(*) as c FROM battery_types').get().c;
  if (typeCount === 0) {
    for (const name of ['LR6', 'LR03']) {
      db.prepare('INSERT OR IGNORE INTO battery_types (id, name, created_by) VALUES (?, ?, ?)').run(uuidv4(), name, adminId);
    }
  }

  // Seed default battery product lines if table is empty
  const lineCount = db.prepare('SELECT COUNT(*) as c FROM battery_product_lines').get().c;
  if (lineCount === 0) {
    for (const name of ['UD+', 'UD', 'HP']) {
      db.prepare('INSERT OR IGNORE INTO battery_product_lines (id, name, created_by) VALUES (?, ?, ?)').run(uuidv4(), name, adminId);
    }
  }
}

function seedAdmin() {
  const existing = db.prepare('SELECT id FROM users WHERE username = ?').get(config.admin.username);
  if (!existing) {
    const { v4: uuidv4 } = require('uuid');
    const passwordHash = bcrypt.hashSync(config.admin.password, 10);
    db.prepare(`
      INSERT INTO users (id, username, password_hash, display_name, role)
      VALUES (?, ?, ?, ?, 'admin')
    `).run(uuidv4(), config.admin.username, passwordHash, config.admin.displayName);
    logger.info('Default admin user created', { username: config.admin.username });
  }
}

module.exports = { initDb, getDb };
