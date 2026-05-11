import React, { createContext, useContext, useState, useEffect, useRef, useCallback } from 'react';
import { notification as antNotification } from 'antd';
import { useAuth } from './AuthContext';
import { useLang } from './LangContext';
import api from '../api';

const NotificationContext = createContext(null);

const MAX_NOTIFICATIONS = 50;

export function NotificationProvider({ children }) {
  const { user } = useAuth();
  const { t } = useLang();
  const [notifications, setNotifications] = useState([]);
  const [unreadCount, setUnreadCount] = useState(0);
  // DB notifications (from /api/notifications)
  const [dbNotifications, setDbNotifications] = useState([]);
  const [dbUnreadCount, setDbUnreadCount] = useState(0);
  const eventSourceRef = useRef(null);
  const reconnectTimeoutRef = useRef(null);
  const pollRef = useRef(null);

  const getNotificationMessage = useCallback((event) => {
    const name = event.fileName || '';
    const userName = event.userName || '';
    switch (event.type) {
      case 'file_added': return `${t('newFileUploaded')}: ${name} (${userName})`;
      case 'file_updated': return `${t('fileUpdated')}: ${name} (${userName})`;
      case 'file_deleted': return `${t('fileDeletedNotif')}: ${name} (${userName})`;
      case 'version_restored': return `${t('versionRestored')}: ${name} (${userName})`;
      case 'file_locked': return `${t('fileLockNotif')}: ${name} (${userName})`;
      case 'file_unlocked': return `${t('fileUnlockNotif')}: ${name} (${userName})`;
      default: return name;
    }
  }, [t]);

  const fetchDbNotifications = useCallback(async () => {
    if (!user) return;
    try {
      const res = await api.get('/notifications', { params: { limit: 20 } });
      setDbNotifications(res.data.data || []);
      setDbUnreadCount(res.data.unreadCount || 0);
    } catch {
      // silently ignore
    }
  }, [user]);

  const connectSSE = useCallback(() => {
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }

    const token = localStorage.getItem('accessToken');
    if (!token) return;

    const es = new EventSource(`/api/notifications/stream?token=${encodeURIComponent(token)}`);
    eventSourceRef.current = es;

    es.onopen = () => {
      // SSE connection established successfully
    };

    es.onmessage = (e) => {
      try {
        const event = JSON.parse(e.data);
        const msg = getNotificationMessage(event);

        // Show Ant Design notification popup
        antNotification.info({
          message: t('notifications'),
          description: msg,
          placement: 'topRight',
          duration: 5,
        });

        // Add to notifications list
        setNotifications(prev => {
          const newItem = {
            id: `${Date.now()}-${Math.random()}`,
            message: msg,
            type: event.type,
            fileId: event.fileId,
            timestamp: event.timestamp || new Date().toISOString(),
            read: false,
          };
          return [newItem, ...prev].slice(0, MAX_NOTIFICATIONS);
        });
        setUnreadCount(prev => prev + 1);
        // Also refresh DB notifications
        fetchDbNotifications();
      } catch {
        // ignore parse errors
      }
    };

    es.onerror = () => {
      es.close();
      eventSourceRef.current = null;
      // Only reconnect if the user still has a valid token
      if (reconnectTimeoutRef.current) clearTimeout(reconnectTimeoutRef.current);
      reconnectTimeoutRef.current = setTimeout(() => {
        const currentToken = localStorage.getItem('accessToken');
        if (currentToken) connectSSE();
      }, 10000);
    };
  }, [getNotificationMessage, t, fetchDbNotifications]);

  useEffect(() => {
    const userId = user?.id;
    if (userId) {
      connectSSE();
      fetchDbNotifications();
      // Poll every 30 seconds
      pollRef.current = setInterval(fetchDbNotifications, 30000);
    } else {
      if (eventSourceRef.current) {
        eventSourceRef.current.close();
        eventSourceRef.current = null;
      }
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
        reconnectTimeoutRef.current = null;
      }
      if (pollRef.current) {
        clearInterval(pollRef.current);
        pollRef.current = null;
      }
      setDbNotifications([]);
      setDbUnreadCount(0);
    }
    return () => {
      if (eventSourceRef.current) {
        eventSourceRef.current.close();
        eventSourceRef.current = null;
      }
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
        reconnectTimeoutRef.current = null;
      }
      if (pollRef.current) {
        clearInterval(pollRef.current);
        pollRef.current = null;
      }
    };
  }, [user?.id, connectSSE, fetchDbNotifications]);

  const markAllRead = useCallback(async () => {
    setNotifications(prev => prev.map(n => ({ ...n, read: true })));
    setUnreadCount(0);
    try {
      await api.put('/notifications/read-all');
      setDbUnreadCount(0);
      setDbNotifications(prev => prev.map(n => ({ ...n, isRead: true })));
    } catch {
      // silently ignore
    }
  }, []);

  const markOneRead = useCallback(async (id) => {
    try {
      await api.put(`/notifications/${id}/read`);
      setDbNotifications(prev => prev.map(n => n.id === id ? { ...n, isRead: true } : n));
      setDbUnreadCount(prev => Math.max(0, prev - 1));
    } catch {
      // silently ignore
    }
  }, []);

  const clearNotifications = useCallback(() => {
    setNotifications([]);
    setUnreadCount(0);
  }, []);

  return (
    <NotificationContext.Provider value={{
      notifications, unreadCount, markAllRead, clearNotifications,
      dbNotifications, dbUnreadCount, markOneRead, fetchDbNotifications,
    }}>
      {children}
    </NotificationContext.Provider>
  );
}

export function useNotifications() {
  return useContext(NotificationContext);
}

