import React from 'react';
import { Tooltip, Tag } from 'antd';
import { UserOutlined, ClockCircleOutlined } from '@ant-design/icons';
import dayjs from 'dayjs';
import relativeTime from 'dayjs/plugin/relativeTime';
import 'dayjs/locale/vi';

dayjs.extend(relativeTime);
dayjs.locale('vi');

const NODE_RADIUS = 8;
const ROW_HEIGHT = 60;
const COL_WIDTH = 30;
const COLORS = ['#1677ff', '#52c41a', '#fa8c16', '#eb2f96', '#722ed1'];

export default function CommitGraph({ versions = [], onSelectVersion, selectedVersionIds = [] }) {
  if (!versions.length) return null;

  const sorted = [...versions].sort((a, b) => a.versionNumber - b.versionNumber);
  const height = sorted.length * ROW_HEIGHT + 20;
  const width = 240;

  return (
    <div style={{ display: 'flex', gap: 0 }}>
      {/* SVG graph */}
      <svg width={COL_WIDTH * 2} height={height} style={{ flexShrink: 0 }}>
        {sorted.map((v, i) => {
          const y = i * ROW_HEIGHT + ROW_HEIGHT / 2 + 10;
          const cx = COL_WIDTH;
          const color = COLORS[0];
          const isSelected = selectedVersionIds.includes(v.id);

          return (
            <g key={v.id}>
              {/* Line to next */}
              {i < sorted.length - 1 && (
                <line
                  x1={cx}
                  y1={y + NODE_RADIUS}
                  x2={cx}
                  y2={y + ROW_HEIGHT - NODE_RADIUS}
                  stroke="#d9d9d9"
                  strokeWidth={2}
                />
              )}
              {/* Node */}
              <circle
                cx={cx}
                cy={y}
                r={isSelected ? NODE_RADIUS + 2 : NODE_RADIUS}
                fill={isSelected ? color : '#fff'}
                stroke={color}
                strokeWidth={2}
                style={{ cursor: 'pointer' }}
                onClick={() => onSelectVersion && onSelectVersion(v)}
              />
              {isSelected && (
                <circle
                  cx={cx}
                  cy={y}
                  r={4}
                  fill="#fff"
                />
              )}
            </g>
          );
        })}
      </svg>

      {/* Labels */}
      <div style={{ flex: 1 }}>
        {sorted.map((v, i) => {
          const isSelected = selectedVersionIds.includes(v.id);
          return (
            <div
              key={v.id}
              onClick={() => onSelectVersion && onSelectVersion(v)}
              style={{
                height: ROW_HEIGHT,
                display: 'flex',
                flexDirection: 'column',
                justifyContent: 'center',
                padding: '4px 8px',
                cursor: 'pointer',
                borderRadius: 6,
                background: isSelected ? '#e6f4ff' : 'transparent',
                border: isSelected ? '1px solid #91caff' : '1px solid transparent',
                transition: 'all 0.2s',
                marginBottom: 2,
              }}
            >
              <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                <Tag color="blue" style={{ margin: 0, fontSize: 11 }}>v{v.versionNumber}</Tag>
                <span style={{
                  fontSize: 13,
                  fontWeight: 500,
                  overflow: 'hidden',
                  textOverflow: 'ellipsis',
                  whiteSpace: 'nowrap',
                  maxWidth: 160,
                }}>
                  {v.commitMessage || `Version ${v.versionNumber}`}
                </span>
              </div>
              <div style={{ display: 'flex', gap: 12, marginTop: 2 }}>
                <span style={{ fontSize: 11, color: '#8c8c8c' }}>
                  <UserOutlined style={{ marginRight: 3 }} />
                  {v.uploadedBy}
                </span>
                <Tooltip title={dayjs(v.createdAt).format('YYYY-MM-DD HH:mm:ss')}>
                  <span style={{ fontSize: 11, color: '#8c8c8c' }}>
                    <ClockCircleOutlined style={{ marginRight: 3 }} />
                    {dayjs(v.createdAt).fromNow()}
                  </span>
                </Tooltip>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
