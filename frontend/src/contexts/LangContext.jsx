import React, { createContext, useContext, useState, useCallback } from 'react';
import vi from '../locales/vi';
import en from '../locales/en';
import zh from '../locales/zh';

const locales = { vi, en, zh };
const VALID_LANGS = ['vi', 'en', 'zh'];

const resolveLocaleValue = (locale, key) => {
  if (!locale) return undefined;
  if (Object.prototype.hasOwnProperty.call(locale, key)) return locale[key];
  return key.split('.').reduce((acc, part) => {
    if (acc && typeof acc === 'object' && part in acc) return acc[part];
    return undefined;
  }, locale);
};

const normalizeLang = (l) => {
  if (!l) return 'vi';
  const normalized = l.trim().toLowerCase();
  return VALID_LANGS.includes(normalized) ? normalized : 'vi';
};

const LangContext = createContext(null);

export function LangProvider({ children }) {
  const [lang, setLang] = useState(() => normalizeLang(localStorage.getItem('lang')));

  const t = useCallback(
    (key, vars = {}) => {
      const locale = locales[lang] || locales.vi;
      let str = resolveLocaleValue(locale, key) ?? resolveLocaleValue(en, key) ?? key;
      if (typeof str !== 'string') str = String(str ?? key);
      Object.entries(vars).forEach(([k, v]) => {
        str = str.replace(`{${k}}`, v);
      });
      return str;
    },
    [lang]
  );

  const switchLang = useCallback((l) => {
    const normalized = normalizeLang(l);
    setLang(normalized);
    localStorage.setItem('lang', normalized);
  }, []);

  return (
    <LangContext.Provider value={{ lang, t, switchLang }}>
      {children}
    </LangContext.Provider>
  );
}

export function useLang() {
  return useContext(LangContext);
}
