// @ts-check
import { defineConfig } from 'astro/config';
import sitemap from '@astrojs/sitemap';

const isGitHubPages = process.env.GITHUB_ACTIONS === 'true';

// https://astro.build/config
export default defineConfig({
  output: 'static',
  site: isGitHubPages ? 'https://shetlandj.github.io' : 'https://shetlandhistory.com',
  ...(isGitHubPages ? {
    base: '/shetland-political-history',
  } : {}),
  integrations: [sitemap()],
});
