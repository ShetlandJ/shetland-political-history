// @ts-check
import { defineConfig } from 'astro/config';

const isGitHubPages = process.env.GITHUB_ACTIONS === 'true';

// https://astro.build/config
export default defineConfig({
  output: 'static',
  ...(isGitHubPages ? {
    site: 'https://shetlandj.github.io',
    base: '/shetland-political-history',
  } : {}),
});
