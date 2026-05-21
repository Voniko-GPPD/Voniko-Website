import React from 'react';

export default function ResponsiveToolbar({
  children,
  justify = 'space-between',
  align = 'center',
  gap = 12,
  style,
}) {
  return (
    <div
      style={{
        display: 'flex',
        flexWrap: 'wrap',
        alignItems: align,
        justifyContent: justify,
        gap,
        width: '100%',
        ...style,
      }}
    >
      {children}
    </div>
  );
}
