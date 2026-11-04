import {defineConfig,mergeConfig} from 'vite';
import base from './vite.config.js';
// Unmatched fixture requests must never reach a developer's real backend.
const proxy={'/api':'http://127.0.0.1:1','/ws':{target:'ws://127.0.0.1:1',ws:true}};
export default defineConfig(env=>mergeConfig(typeof base==='function'?base(env):base,{server:{proxy},preview:{proxy}}));
