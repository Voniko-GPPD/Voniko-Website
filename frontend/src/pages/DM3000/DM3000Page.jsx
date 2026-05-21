import React from 'react';
import DM2000Page from '../DM2000/DM2000Page';

// DM3000 reuses the entire DM2000 page implementation; only the
// `module` prop differs (drives API base URL, page title, mA unit
// display, etc.).  Keeping a thin wrapper here means the BatteryDMP
// tabs file can mount DM3000 in the same way as DM2000.
export default function DM3000Page() {
  return <DM2000Page module="dm3000" />;
}
