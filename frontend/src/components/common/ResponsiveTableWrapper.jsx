import React from 'react';

export default function ResponsiveTableWrapper({ children, minWidth = 960, style }) {
  return (
    <div
      style={{
        width: '100%',
        overflowX: 'auto',
        overflowY: 'hidden',
        WebkitOverflowScrolling: 'touch',
        borderRadius: 8,
        ...style,
      }}
    >
      <div style={{ minWidth }}>
        {children}
      </div>
    </div>
  );
}
