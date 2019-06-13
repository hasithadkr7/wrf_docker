from scipy.spatial import Voronoi, voronoi_plot_2d
from descartes import PolygonPatch
from matplotlib import pyplot as plt
import numpy as np
import shapely.geometry
import shapely.ops
import shapely
from shapely.geometry import Point
import os

import os, osgeo, math, numpy as np
from osgeo import gdal, ogr, osr
from scipy.spatial import Delaunay

p = [[b'Colombo', 6.898158, 79.8653],
     [b'IBATTARA3', 6.89, 79.86],
     [b'Isurupaya', 6.89, 79.92],
     [b'Borella', 6.93, 79.86],
     [b'Kompannaveediya', 6.92, 79.85],
     ]

points = np.array(p, dtype=object)


def voronoi_finite_polygons_2d(vor, radius=None):
    """
    Reconstruct infinite voronoi regions in a 2D diagram to finite
    regions.

    Parameters
    ----------
    vor : Voronoi
        Input diagram
    radius : float, optional
        Distance to 'points at infinity'.

    Returns
    -------
    regions : list of tuples
        Indices of vertices in each revised Voronoi regions.
    vertices : list of tuples
        Coordinates for revised Voronoi vertices. Same as coordinates
        of input vertices, with 'points at infinity' appended to the
        end.

    """

    if vor.points.shape[1] != 2:
        raise ValueError("Requires 2D input")

    new_regions = []
    new_vertices = vor.vertices.tolist()

    center = vor.points.mean(axis=0)
    if radius is None:
        radius = vor.points.ptp().max()

    # Construct a map containing all ridges for a given point
    all_ridges = {}
    for (p1, p2), (v1, v2) in zip(vor.ridge_points, vor.ridge_vertices):
        all_ridges.setdefault(p1, []).append((p2, v1, v2))
        all_ridges.setdefault(p2, []).append((p1, v1, v2))

    # Reconstruct infinite regions
    for p1, region in enumerate(vor.point_region):
        vertices = vor.regions[region]

        if all(v >= 0 for v in vertices):
            # finite region
            new_regions.append(vertices)
            continue

        # reconstruct a non-finite region
        ridges = all_ridges[p1]
        new_region = [v for v in vertices if v >= 0]

        for p2, v1, v2 in ridges:
            if v2 < 0:
                v1, v2 = v2, v1
            if v1 >= 0:
                # finite ridge: already in the region
                continue

            # Compute the missing endpoint of an infinite ridge

            t = vor.points[p2] - vor.points[p1] # tangent
            t /= np.linalg.norm(t)
            n = np.array([-t[1], t[0]])  # normal

            midpoint = vor.points[[p1, p2]].mean(axis=0)
            direction = np.sign(np.dot(midpoint - center, n)) * n
            far_point = vor.vertices[v2] + direction * radius

            new_region.append(len(new_vertices))
            new_vertices.append(far_point.tolist())

        # sort region counterclockwise
        vs = np.asarray([new_vertices[v] for v in new_region])
        c = vs.mean(axis=0)
        angles = np.arctan2(vs[:,1] - c[1], vs[:,0] - c[0])
        new_region = np.array(new_region)[np.argsort(angles)]

        # finish
        new_regions.append(new_region.tolist())

    return new_regions, np.asarray(new_vertices)


vor = Voronoi(np.flip(points[:, 1:3], 1))
#
# regions, vertices = voronoi_finite_polygons_2d(vor)
#
# for region in regions:
#     polygon = vertices[region]
#     plt.fill(*zip(*polygon), alpha=0.4)
#
# # plt.plot(points[:,0], points[:,1], 'ko')
# # plt.xlim(vor.min_bound[0] - 0.1, vor.max_bound[0] + 0.1)
# # plt.ylim(vor.min_bound[1] - 0.1, vor.max_bound[1] + 0.1)
#
# plt.show()
#
# vorShp = '/home/curw/gis/temp/voronoi.shp'
# print("- creating output polygon shp", vorShp.split('/')[-1])
# if os.path.exists(vorShp):
#     os.remove(vorShp)
# drv = ogr.GetDriverByName('ESRI Shapefile')
# outShp = drv.CreateDataSource(vorShp)
# layer = outShp.CreateLayer('', None, ogr.wkbPolygon)
# layer.CreateField(ogr.FieldDefn('Id', ogr.OFTInteger))
# layerDefn = layer.GetLayerDefn()
#
# f = '/home/curw/gis/Kelani_lower_basin/Lower_basin.shp'
# shp = drv.Open(f)
# l = shp.GetLayer(0)
# ll = l.GetField(0)
#
# print("- building Voronoi polygons around point...")
# i = 1
# for region in regions:
#     polygon = vertices[region]
#
#     poly = ogr.Geometry(ogr.wkbPolygon)
#     ring = ogr.Geometry(ogr.wkbLinearRing)
#
#     for node in polygon:
#         ring.AddPoint(node[0], node[1])
#
#     ring.AddPoint(polygon[0][0], polygon[0][1])
#     poly.AddGeometry(ring)
#     feat = ogr.Feature(layerDefn)
#     feat.SetField('Id', i)
#     feat.SetGeometry(poly)
#     layer.CreateFeature(feat)
#     i += 1
#

import geopandas as gpd

g1 = gpd.GeoDataFrame.from_file("/home/curw/gis/klb-wgs84/klb-wgs84.shp")
g2 = gpd.GeoDataFrame.from_file("/home/curw/gis/temp/voronoi.shp")
data = []
for index, crim in g1.iterrows():
    for index2, popu in g2.iterrows():
        if crim['geometry'].intersects(popu['geometry']):
            data.append(
                {'geometry': crim['geometry'].intersection(popu['geometry']),
                 'area': crim['geometry'].intersection(popu['geometry']).area})

df = gpd.GeoDataFrame(data, columns=['geometry', 'area'])
df.to_file('/home/curw/gis/temp/intersection.shp')


#
#
# voronoi_plot_2d(vor, 10.0)
#
# points = []
# for point in vor.ridge_points:
#     point
#
# lines = [
#     shapely.geometry.LineString(vor.vertices[line])
#     for line in vor.ridge_vertices
# ]
# point = Point(6.91, 79.91)
# for poly in shapely.ops.polygonize(lines):
#     print(point.within(poly))
#
# shapely.geometry.Polygon()
# polyy = shapely.ops.polygonize(lines)
#
#
#
#
#
#
# # fig = plt.figure(1)
# PolygonPatch(polyy)