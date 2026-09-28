// Boundary for the entity [slug] layouts' notFound(). Without one inside
// the group, a layout-level 404 has nothing to render into and ships an
// empty client-rendered shell; with it the page is server-rendered, nav
// included.
export { default } from "@/app/not-found";
