import React from 'react';
import ReactDOM from 'react-dom/client';
import App from './App';
import './index.css';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc';
import relativeTime from 'dayjs/plugin/relativeTime';
import 'dayjs/locale/vi';

dayjs.extend(utc);
dayjs.extend(relativeTime);
dayjs.locale('vi');

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);
