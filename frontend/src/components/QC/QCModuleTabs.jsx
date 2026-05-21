import React, { useMemo } from 'react';
import { Tabs } from 'antd';
import { useLocation, useNavigate } from 'react-router-dom';
import { useAuth } from '../../contexts/AuthContext';
import { useLang } from '../../contexts/LangContext';

export default function QCModuleTabs() {
  const navigate = useNavigate();
  const location = useLocation();
  const { isAdmin, isQC, isQCManager } = useAuth();
  const { t } = useLang();

  const items = useMemo(() => {
    const next = [
      { key: '/qc/dashboard', label: t('qc.nav_dashboard') },
      { key: '/qc/entry', label: t('qc.nav_entry') },
      { key: '/qc/dictionaries', label: t('qc.nav_dictionaries'), visible: isAdmin || isQC || isQCManager },
    ];

    return next.filter((item) => item.visible !== false);
  }, [isAdmin, isQC, isQCManager, t]);

  const activeKey = useMemo(() => {
    const matched = items.find((item) => location.pathname.startsWith(item.key));
    return matched?.key || items[0]?.key;
  }, [items, location.pathname]);

  return (
    <Tabs
      activeKey={activeKey}
      items={items}
      onChange={(key) => navigate(key)}
      style={{ marginBottom: 8 }}
    />
  );
}
