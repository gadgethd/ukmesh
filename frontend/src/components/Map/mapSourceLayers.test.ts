import assert from 'node:assert/strict';
import test from 'node:test';
import type maplibregl from 'maplibre-gl';
import { installMapSourcesAndLayers } from './mapSourceLayers.js';
import { MAP_OVERLAY_COLORS } from './mapConfig.js';

function rgb(hex: string): [number, number, number] {
  return [1, 3, 5].map((offset) => Number.parseInt(hex.slice(offset, offset + 2), 16)) as [number, number, number];
}

function luminance(color: [number, number, number]): number {
  const [r, g, b] = color.map((channel) => {
    const value = channel / 255;
    return value <= 0.04045 ? value / 12.92 : ((value + 0.055) / 1.055) ** 2.4;
  });
  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
}

function contrast(foreground: string, background: string, opacity = 1): number {
  const fg = rgb(foreground);
  const bg = rgb(background);
  const composited = fg.map((channel, index) => channel * opacity + bg[index]! * (1 - opacity)) as [number, number, number];
  const foregroundLuminance = luminance(composited);
  const backgroundLuminance = luminance(bg);
  return (Math.max(foregroundLuminance, backgroundLuminance) + 0.05)
    / (Math.min(foregroundLuminance, backgroundLuminance) + 0.05);
}

test('map layers avoid observer rings and raster-style glyph dependencies', () => {
  const sourceIds: string[] = [];
  const layers: maplibregl.LayerSpecification[] = [];
  const map = {
    addSource: (id: string) => {
      sourceIds.push(id);
    },
    addLayer: (layer: maplibregl.LayerSpecification) => {
      layers.push(layer);
    },
  } as unknown as maplibregl.Map;

  installMapSourcesAndLayers(map, { showLinks: true, mapLight: false });

  assert.equal(sourceIds.includes('observer-health'), false);
  assert.equal(layers.some((layer) => layer.id === 'observer-health-rings'), false);
  assert.equal(layers.some((layer) => (
    layer.type === 'symbol'
    && layer.layout != null
    && 'text-field' in layer.layout
  )), false);
  assert.equal(layers.some((layer) => layer.id === 'planned-pins-dot'), true);
  assert.doesNotMatch(JSON.stringify(layers), /is_inferred/);
});

test('map overlay markers and links retain 3:1 contrast in both themes', () => {
  const nodeColors = [
    'repeater', 'companion', 'roomServer', 'sensor', 'replay',
    'stale', 'linkOnlyStale', 'clashRelay', 'clashOffender',
  ] as const;
  const linkColors = ['linkUnknown', 'linkGood', 'linkMarginal', 'linkPoor'] as const;
  const selectedColors = ['selected', 'selectedStroke'] as const;
  const backgrounds = { dark: '#080d14', light: '#edf2f7' } as const;

  for (const theme of ['dark', 'light'] as const) {
    const palette = MAP_OVERLAY_COLORS[theme];
    for (const key of nodeColors) {
      assert.ok(
        contrast(palette[key], backgrounds[theme], palette.dimmedOpacity) >= 3,
        `${theme} ${key} should remain visible when dimmed`,
      );
    }
    for (const key of linkColors) {
      const opacity = key === 'linkUnknown' ? 0.75 : 0.9;
      assert.ok(
        contrast(palette[key], backgrounds[theme], opacity) >= 3,
        `${theme} ${key} should remain visible at its rendered opacity`,
      );
    }
    for (const key of selectedColors) {
      assert.ok(contrast(palette[key], backgrounds[theme]) >= 3, `${theme} ${key} should be visible`);
    }
  }
});
