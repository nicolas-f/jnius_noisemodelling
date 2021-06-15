import jnius_config
import os

jnius_config.add_options('-Xmx4096m')
jnius_config.set_classpath('dependencies/*')
from jnius import autoclass

# Imports
SFSUtilities = autoclass('org.h2gis.utilities.SFSUtilities')
H2GISDBFactory = autoclass('org.h2gis.functions.factory.H2GISDBFactory')
UUID = autoclass('java.util.UUID')
Driver = autoclass('org.h2.Driver')
DriverManager = autoclass('java.sql.DriverManager')
H2GISFunctions = autoclass('org.h2gis.functions.factory.H2GISFunctions')
Import_File = autoclass('org.noise_planet.noisemodelling.wps.Import_and_Export.Import_File')
LinkedHashMap = autoclass('java.util.LinkedHashMap')
Sql = autoclass('groovy.sql.Sql')
JDBCUtilities = autoclass('org.h2gis.utilities.JDBCUtilities')
Regular_Grid = autoclass('org.noise_planet.noisemodelling.wps.Receivers.Regular_Grid')
Noise_level_from_source = autoclass('org.noise_planet.noisemodelling.wps.NoiseModelling.Noise_level_from_source')
Export_Table = autoclass('org.noise_planet.noisemodelling.wps.Import_and_Export.Export_Table')


def to_groovy_map(obj):
    map = LinkedHashMap(len(obj))
    for key, val in obj.items():
        if isinstance(val, dict):
            map.put(key, to_groovy_map(val))
        else:
            map.put(key, val)
    return map


def get_connection():
    # load h2 database driver
    Driver.load()
    connection = DriverManager.getConnection("jdbc:h2:" + os.path.abspath("localdb"),
                                             "sa", "sa")
    if not JDBCUtilities.tableExists(connection, 'SPATIAL_REF_SYS'):
        # load H2GIS extension
        H2GISFunctions.load(connection)
    connection = SFSUtilities.wrapConnection(connection)
    return connection


def main():
    connection = get_connection()
    try:
        sql = Sql(connection)
        Import_File().exec(connection, to_groovy_map({"pathFile": "data/buildings.shp"}))
        Import_File().exec(connection, to_groovy_map({"pathFile": "data/lw_roads.shp"}))

        Regular_Grid().exec(connection, to_groovy_map({"sourcesTableName": "LW_ROADS",
                                                       "delta": 50,
                                                       "buildingTableName": "BUILDINGS"}))

        print(Noise_level_from_source().exec(connection, to_groovy_map({"tableSources": "LW_ROADS",
                                                                        "tableBuilding": "BUILDINGS",
                                                                        "tableReceivers": "RECEIVERS",
                                                                        "confReflOrder": 1,
                                                                        "confDiffVertical": True,
                                                                        "confDiffHorizontal": True,
                                                                        "confSkipLevening": True,
                                                                        "confSkipLnight": True,
                                                                        "confSkipLden": True})))

        Export_Table().exec(connection, to_groovy_map({"exportPath": "LDAY_GEOM.shp", "tableToExport": "LDAY_GEOM"}))
    finally:
        connection.close()


if __name__ == '__main__':
    main()
