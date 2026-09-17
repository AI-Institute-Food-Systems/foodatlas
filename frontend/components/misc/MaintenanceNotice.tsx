// Env-gated maintenance bar. Renders only when NEXT_PUBLIC_MAINTENANCE_NOTICE
// is set at build time, so flipping the Vercel env var and redeploying is
// the whole on/off switch — no code change, no hotfix PR. Pinned to the
// bottom edge so it never has to negotiate with the fixed Navbar's height,
// and above the SearchBar portal (z-50) / open mobile menu (z-[60]) so it
// cannot be hidden behind either.
const MaintenanceNotice = () => {
  const message = process.env.NEXT_PUBLIC_MAINTENANCE_NOTICE;
  if (!message) return null;
  return (
    <div
      role="status"
      aria-live="polite"
      className="fixed bottom-0 inset-x-0 z-[70] px-4 md:px-24 py-2 bg-accent-600 text-white text-sm text-center shadow-lg"
      style={{ paddingBottom: "max(0.5rem, env(safe-area-inset-bottom))" }}
    >
      {message}
    </div>
  );
};

export default MaintenanceNotice;
