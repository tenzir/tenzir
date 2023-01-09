import { sveltekit } from '@sveltejs/kit/vite';
import type { UserConfig } from 'vite';

import Unocss from 'unocss/vite';

import { presetUno } from 'unocss';
import presetIcons from '@unocss/preset-icons';

const config: UserConfig = {
  plugins: [
    sveltekit(),

    Unocss({
      presets: [presetUno(), presetIcons({})]
    })
  ]
};

export default config;
