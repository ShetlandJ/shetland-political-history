// @ts-check
import { defineConfig } from 'astro/config';
import sitemap from '@astrojs/sitemap';

// https://astro.build/config
export default defineConfig({
  output: 'static',
  site: 'https://shetlandhistory.com',
  // Working pages are reachable but kept out of search engines (they also set noindex).
  integrations: [sitemap({ filter: (page) => !page.includes('/data-review') })],
});
