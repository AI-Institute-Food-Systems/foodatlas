// The umami tracker script (app/layout.tsx) attaches this global in prod.
declare global {
  interface Window {
    umami?: {
      track(name: string, data?: Record<string, unknown>): void;
    };
  }
}

export {};
