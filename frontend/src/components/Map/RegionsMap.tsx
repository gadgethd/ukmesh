import { useCallback, useEffect, useState } from 'react';
import * as maplibregl from 'maplibre-gl';
import { MapLibreMap } from './MapLibreMap.js';
import { DEFAULT_CENTER } from './mapConfig.js';
import {
  buildRegionAreasGeoJSON,
  buildRegionRepeatersGeoJSON,
  buildUnattributedGeoJSON,
  type RegionRepeaterFilters,
} from './regionOverlay.js';
import type { RegionRepeater, RegionSummary, UnattributedCluster } from '../../pages/regionsModel.js';

const REGION_AREAS_FILL_LAYER = 'region-areas-fill';
const REGION_AREAS_LINE_LAYER = 'region-areas-line';
const REGION_REPEATER_HIT_LAYER = 'region-repeater-hit';
const REGION_REPEATER_HALO_LAYER = 'region-repeater-halo';
const REGION_REPEATER_LAYER = 'region-repeater';
const REGIONS_INITIAL_VIEW = { lat: DEFAULT_CENTER[0], lon: -2.3, zoom: 5.7 } as const;
const UNATTRIBUTED_SOURCE = 'region-unattributed';
const UNATTRIBUTED_FILL_LAYER = 'region-unattributed-fill';
const UNATTRIBUTED_LINE_LAYER = 'region-unattributed-line';
const UNATTRIBUTED_LABEL_LAYER = 'region-unattributed-label';

type RegionsMapProps = {
  regions: readonly RegionSummary[];
  includeLowConfidence: boolean;
  repeaters: readonly RegionRepeater[];
  shapesVisible: boolean;
  repeatersVisible: boolean;
  unattributed: readonly UnattributedCluster[];
  unattributedVisible: boolean;
  roleFilter: 'repeaters' | 'all';
  statusFilter: 'all' | 'green' | 'red';
  regionFilter: string | null;
  memberIds: ReadonlySet<string> | null;
  selectedRegion: string | null;
  selectedNodeId: string | null;
  network?: string;
  observer?: string;
  privacyGeneration: number;
  onRegionSelect: (region: string) => void;
  onRepeaterSelect: (nodeId: string) => void;
};

type RegionsMapOverlayProps = Omit<RegionsMapProps, 'network' | 'observer' | 'privacyGeneration' | 'includeLowConfidence'> & {
  map: maplibregl.Map;
};

function getFirstSymbolLayerId(map: maplibregl.Map): string | undefined {
  for (const layer of map.getStyle().layers) {
    if (layer.type === 'symbol') return layer.id;
  }
  return undefined;
}

function addRegionsLayer(map: maplibregl.Map, layer: maplibregl.LayerSpecification): void {
  if (map.getLayer(layer.id)) return;
  const beforeId = getFirstSymbolLayerId(map);
  if (beforeId) map.addLayer(layer, beforeId);
  else map.addLayer(layer);
}

const regionAreaBase = {
  type: 'fill' as const,
  source: 'region-areas',
};

const regionLineBase = {
  type: 'line' as const,
  source: 'region-areas',
  layout: {
    'line-cap': 'round' as const,
    'line-join': 'round' as const,
  },
};

const regionRepeaterBase = {
  type: 'circle' as const,
  source: 'region-repeaters',
};

function RegionsMapOverlay({
  map,
  regions,
  repeaters,
  shapesVisible,
  repeatersVisible,
  unattributed,
  unattributedVisible,
  roleFilter,
  statusFilter,
  regionFilter,
  memberIds,
  selectedRegion,
  selectedNodeId,
  onRegionSelect,
  onRepeaterSelect,
}: RegionsMapOverlayProps): null {
  const [styleGeneration, setStyleGeneration] = useState(0);
  useEffect(() => {
    const install = () => {
      // A replacement style can still be fetching when this effect mounts.
      // getStyle() is absent until its JSON is installed; isStyleLoaded() also
      // waits for tiles/images and is too strict for adding application layers.
      if (!map.getStyle()) return;
      if (!map.getSource('region-areas')) map.addSource('region-areas', {
        type: 'geojson',
        data: { type: 'FeatureCollection', features: [] },
      });
      if (!map.getSource('region-repeaters')) map.addSource('region-repeaters', {
        type: 'geojson',
        data: { type: 'FeatureCollection', features: [] },
      });
      if (!map.getSource(UNATTRIBUTED_SOURCE)) map.addSource(UNATTRIBUTED_SOURCE, {
        type: 'geojson',
        data: { type: 'FeatureCollection', features: [] },
      });

      addRegionsLayer(map, {
        ...regionAreaBase,
        id: REGION_AREAS_FILL_LAYER,
        paint: {
          'fill-color': ['get', 'color'],
          'fill-opacity': ['get', 'fill_opacity'],
        },
      });
      addRegionsLayer(map, {
        ...regionLineBase,
        id: REGION_AREAS_LINE_LAYER,
        paint: {
          'line-color': ['get', 'color'],
          'line-width': ['get', 'line_width'],
          'line-opacity': ['get', 'line_opacity'],
        },
      });
      addRegionsLayer(map, {
        ...regionRepeaterBase,
        id: REGION_REPEATER_HIT_LAYER,
        paint: {
          'circle-radius': ['interpolate', ['linear'], ['zoom'], 5, 9, 8, 11, 12, 15],
          'circle-color': '#ffffff',
          'circle-opacity': 0,
          'circle-stroke-opacity': 0,
        },
      });
      addRegionsLayer(map, {
        ...regionRepeaterBase,
        id: REGION_REPEATER_HALO_LAYER,
        paint: {
          'circle-radius': ['+', ['get', 'radius'], 5],
          'circle-color': [
            'case',
            ['get', 'selected'], '#f8fafc',
            ['==', ['get', 'status'], 'green'], '#22c55e',
            '#ef4444',
          ],
          'circle-opacity': ['case', ['get', 'selected'], 0.3, 0.08],
          'circle-blur': 0.55,
        },
      });
      addRegionsLayer(map, {
        ...regionRepeaterBase,
        id: REGION_REPEATER_LAYER,
        paint: {
          'circle-radius': [
            'interpolate', ['linear'], ['zoom'],
            5, ['case', ['==', ['get', 'status'], 'green'], 3.6, 3],
            8, ['case', ['==', ['get', 'status'], 'green'], 5.4, 4.2],
            12, ['case', ['==', ['get', 'status'], 'green'], 7.5, 6],
          ],
          'circle-color': [
            'case',
            ['get', 'selected'], '#f8fafc',
            ['==', ['get', 'status'], 'green'], '#22c55e',
            '#ef4444',
          ],
          'circle-opacity': [
            'case',
            ['get', 'selected'], 1,
            ['==', ['get', 'tier'], 'confirmed'], 0.95,
            ['==', ['get', 'tier'], 'probable'], 0.8,
            ['==', ['get', 'tier'], 'single'], 0.55,
            0.72,
          ],
          'circle-stroke-color': [
            'case',
            ['get', 'selected'], '#ffffff',
            ['==', ['get', 'status'], 'green'], '#052e16',
            '#450a0a',
          ],
          'circle-stroke-width': ['case', ['get', 'selected'], 2.4, 1.1],
          'circle-stroke-opacity': 0.92,
        },
      });

      addRegionsLayer(map, {
        id: UNATTRIBUTED_FILL_LAYER,
        type: 'fill',
        source: UNATTRIBUTED_SOURCE,
        paint: {
          'fill-color': '#94a3b8',
          'fill-opacity': 0.07,
        },
      });
      addRegionsLayer(map, {
        id: UNATTRIBUTED_LINE_LAYER,
        type: 'line',
        source: UNATTRIBUTED_SOURCE,
        layout: { 'line-cap': 'round', 'line-join': 'round' },
        paint: {
          'line-color': '#cbd5e1',
          'line-width': 1.3,
          'line-opacity': 0.8,
          'line-dasharray': [2, 2],
        },
      });
      addRegionsLayer(map, {
        id: UNATTRIBUTED_LABEL_LAYER,
        type: 'symbol',
        source: UNATTRIBUTED_SOURCE,
        layout: {
          'text-field': ['get', 'label'],
          'text-size': 10.5,
          'text-anchor': 'center',
          'text-allow-overlap': false,
        },
        paint: {
          'text-color': '#e2e8f0',
          'text-halo-color': '#020617',
          'text-halo-width': 1.6,
        },
      });

      // Reapply current data, selection and visibility even if their React
      // dependencies did not change while setStyle removed the old sources.
      setStyleGeneration(generation => generation + 1);
    };
    map.on('style.load', install);
    install();

    const handleRepeaterClick = (event: maplibregl.MapLayerMouseEvent) => {
      const nodeId = event.features?.[0]?.properties?.['node_id'];
      if (typeof nodeId === 'string' && nodeId.length > 0) onRepeaterSelect(nodeId);
    };
    const handleAreaClick = (event: maplibregl.MapLayerMouseEvent) => {
      // A repeater dot sitting inside a shape must win over the shape itself.
      if (map.queryRenderedFeatures(event.point, { layers: [REGION_REPEATER_HIT_LAYER] }).length > 0) return;
      const region = event.features?.[0]?.properties?.['region'];
      if (typeof region === 'string' && region.length > 0) onRegionSelect(region);
    };
    const handlePointerEnter = () => { map.getCanvas().style.cursor = 'pointer'; };
    const handlePointerLeave = () => { map.getCanvas().style.cursor = ''; };
    map.on('click', REGION_REPEATER_HIT_LAYER, handleRepeaterClick);
    map.on('click', REGION_AREAS_FILL_LAYER, handleAreaClick);
    map.on('mouseenter', REGION_REPEATER_HIT_LAYER, handlePointerEnter);
    map.on('mouseleave', REGION_REPEATER_HIT_LAYER, handlePointerLeave);
    map.on('mouseenter', REGION_AREAS_FILL_LAYER, handlePointerEnter);
    map.on('mouseleave', REGION_AREAS_FILL_LAYER, handlePointerLeave);

    const navigation = new maplibregl.NavigationControl({ showCompass: false });
    map.addControl(navigation, 'top-right');

    return () => {
      map.off('style.load', install);
      map.off('click', REGION_REPEATER_HIT_LAYER, handleRepeaterClick);
      map.off('click', REGION_AREAS_FILL_LAYER, handleAreaClick);
      map.off('mouseenter', REGION_REPEATER_HIT_LAYER, handlePointerEnter);
      map.off('mouseleave', REGION_REPEATER_HIT_LAYER, handlePointerLeave);
      map.off('mouseenter', REGION_AREAS_FILL_LAYER, handlePointerEnter);
      map.off('mouseleave', REGION_AREAS_FILL_LAYER, handlePointerLeave);
      try { map.removeControl(navigation); } catch { /* map may already be removing */ }
      try {
        for (const layerId of [
          REGION_REPEATER_LAYER,
          REGION_REPEATER_HALO_LAYER,
          REGION_REPEATER_HIT_LAYER,
          REGION_AREAS_LINE_LAYER,
          REGION_AREAS_FILL_LAYER,
          UNATTRIBUTED_LABEL_LAYER,
          UNATTRIBUTED_LINE_LAYER,
          UNATTRIBUTED_FILL_LAYER,
        ]) {
          if (map.getLayer(layerId)) map.removeLayer(layerId);
        }
        if (map.getSource('region-repeaters')) map.removeSource('region-repeaters');
        if (map.getSource('region-areas')) map.removeSource('region-areas');
        if (map.getSource(UNATTRIBUTED_SOURCE)) map.removeSource(UNATTRIBUTED_SOURCE);
      } catch { /* MapLibreMap owns final map teardown */ }
    };
  }, [map, onRegionSelect, onRepeaterSelect]);

  useEffect(() => {
    if (!map.getStyle()) return;
    const areasSource = map.getSource('region-areas') as maplibregl.GeoJSONSource | undefined;
    areasSource?.setData(buildRegionAreasGeoJSON(regions, selectedRegion));
  }, [map, styleGeneration, regions, selectedRegion]);

  useEffect(() => {
    if (!map.getStyle()) return;
    const repeatersSource = map.getSource('region-repeaters') as maplibregl.GeoJSONSource | undefined;
    if (!repeatersSource) return;
    const filters: RegionRepeaterFilters = {
      roleFilter,
      statusFilter,
      regionFilter,
      memberIds,
      selectedNodeId,
    };
    const featureCollection = buildRegionRepeatersGeoJSON(repeaters, filters);
    // Radius is a data-driven paint input, so carry a fixed per-feature radius.
    repeatersSource.setData({
      ...featureCollection,
      features: featureCollection.features.map((feature) => ({
        ...feature,
        properties: { ...feature.properties, radius: feature.properties['status'] === 'green' ? 4.5 : 3.8 },
      })),
    });
  }, [map, styleGeneration, repeaters, roleFilter, statusFilter, regionFilter, memberIds, selectedNodeId]);

  useEffect(() => {
    if (!map.getStyle()) return;
    const unattributedSource = map.getSource(UNATTRIBUTED_SOURCE) as maplibregl.GeoJSONSource | undefined;
    unattributedSource?.setData(buildUnattributedGeoJSON(unattributed));
  }, [map, styleGeneration, unattributed]);

  useEffect(() => {
    if (!map.getStyle()) return;
    for (const [layerId, visible] of [
      [REGION_AREAS_FILL_LAYER, shapesVisible],
      [REGION_AREAS_LINE_LAYER, shapesVisible],
      [REGION_REPEATER_HIT_LAYER, repeatersVisible],
      [REGION_REPEATER_HALO_LAYER, repeatersVisible],
      [REGION_REPEATER_LAYER, repeatersVisible],
      [UNATTRIBUTED_FILL_LAYER, unattributedVisible],
      [UNATTRIBUTED_LINE_LAYER, unattributedVisible],
      [UNATTRIBUTED_LABEL_LAYER, unattributedVisible],
    ] as const) {
      if (map.getLayer(layerId)) {
        map.setLayoutProperty(layerId, 'visibility', visible ? 'visible' : 'none');
      }
    }
  }, [map, styleGeneration, shapesVisible, repeatersVisible, unattributedVisible]);

  return null;
}

export function RegionsMap({
  regions,
  includeLowConfidence,
  repeaters,
  shapesVisible,
  repeatersVisible,
  unattributed,
  unattributedVisible,
  roleFilter,
  statusFilter,
  regionFilter,
  memberIds,
  selectedRegion,
  selectedNodeId,
  network,
  observer,
  privacyGeneration,
  onRegionSelect,
  onRepeaterSelect,
}: RegionsMapProps): React.ReactElement {
  const [map, setMap] = useState<maplibregl.Map | null>(null);
  const handleMapReady = useCallback((nextMap: maplibregl.Map) => setMap(nextMap), []);
  const shapedRegions = regions.filter((region) => region.members.some((member) => member.lat != null && member.lon != null)).length;
  const locatedRepeaters = repeaters.filter((repeater) => Number.isFinite(repeater.lat) && Number.isFinite(repeater.lon)).length;

  return (
    <div className="regions-page__map" aria-label="MeshCore region scoping map">
      <MapLibreMap
        showLinks={false}
        showTerrain={false}
        showClientNodes={false}
        showHexClashes={false}
        maxHexClashHops={3}
        viewshedEnabled={false}
        rfCoverageEnabled={false}
        initialView={REGIONS_INITIAL_VIEW}
        mapLight={false}
        network={network}
        observer={observer}
        privacyGeneration={privacyGeneration}
        onMapReady={handleMapReady}
        showMapChrome={false}
      />
      {map && (
        <RegionsMapOverlay
          map={map}
          regions={regions}
          repeaters={repeaters}
          shapesVisible={shapesVisible}
          repeatersVisible={repeatersVisible}
          unattributed={unattributed}
          unattributedVisible={unattributedVisible}
          roleFilter={roleFilter}
          statusFilter={statusFilter}
          regionFilter={regionFilter}
          memberIds={memberIds}
          selectedRegion={selectedRegion}
          selectedNodeId={selectedNodeId}
          onRegionSelect={onRegionSelect}
          onRepeaterSelect={onRepeaterSelect}
        />
      )}
      <div className="regions-page__map-meta" aria-live="polite">
        <strong>Region scoping</strong>
        <span>{shapedRegions} region shapes · {locatedRepeaters} mapped repeaters{unattributed.length > 0 ? ` · ${unattributed.length} unnamed footprints` : ''}</span>
        <span>
          {regionFilter
            ? `Filtered to ${regionFilter}`
            : includeLowConfidence
              ? 'All observed regions'
              : 'Established regions · low-confidence hidden'}
        </span>
      </div>
      <div className="regions-page__map-legend" aria-label="Regions map legend">
        <span><i className="regions-page__legend-swatch regions-page__legend-swatch--green" /> Region-capable repeater · solid = strong evidence</span>
        <span><i className="regions-page__legend-swatch regions-page__legend-swatch--red" /> No scoped relay observed</span>
        <span><i className="regions-page__legend-swatch regions-page__legend-swatch--area" /> Region footprint (convex hull of members relaying ≥10% of region traffic; bridge repeaters excluded)</span>
        <span><i className="regions-page__legend-swatch regions-page__legend-swatch--unnamed" /> Unnamed footprint — scoped traffic whose region name cannot be derived (private regions by design)</span>
      </div>
    </div>
  );
}
