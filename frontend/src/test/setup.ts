import '@testing-library/jest-dom';

// Mock ResizeObserver for recharts ResponsiveContainer
global.ResizeObserver = class {
  observe() {}
  unobserve() {}
  disconnect() {}
};
