from pathlib import Path
import get_urls
import download_viirs
from extract_rasters import ExtractFromTiles
from concurrent.futures import ThreadPoolExecutor
import subprocess

def main(url, year):
    download = download_viirs.DownloadViirs(url)
    download.download_rasters()
    download.open_tgz()
    if 'ANNUAL_' not in download.month:
        url_extent = url.split('/')[-1].split('_')[3]
        extents_countries = {'75N180W': ['HTI'], '75N060W': ['GHA'], '00N060W': ['MOZ', 'NAM'], '75N060E': ['NPL']}
        for iso in extents_countries[url_extent]:
            raster_rad = [x for x in download.download_path.iterdir() if url_extent in x.name if x.name.endswith('rade9h.tif')][0]
            raster_cvg = [x for x in download.download_path.iterdir() if url_extent in x.name if x.name.endswith('cvg.tif')][0]
            shp = BASEDIR.joinpath(f'shps/{iso}/{iso.lower()}_level0_2000_2020.shp')
            outfolder = download.download_path.joinpath(f'{iso}')
            outraster_rad = outfolder.joinpath(f'{iso}_{download.month}_rad.tif')
            outraster_cvg = outfolder.joinpath(f'{iso}_{download.month}_cvg.tif')
            gdal_cmd_rad = f'gdal_edit.py -a_nodata -99999 {str(raster_rad)}'
            gdal_cmd_cvg = f'gdal_edit.py -a_nodata 255 {str(raster_cvg)}'
            subprocess.call(gdal_cmd_rad, shell=True)
            subprocess.call(gdal_cmd_cvg, shell=True)
            if not outfolder.exists():
                outfolder.mkdir(parents=True, exist_ok=True)
            if not outraster_rad.exists():
                ExtractFromTiles(raster_rad, shp, outraster_rad)
            if not outraster_cvg.exists():
                ExtractFromTiles(raster_cvg, shp, outraster_cvg)
        raster_rad.unlink()
        raster_cvg.unlink() 


if __name__ == "__main__":
    BASEDIR = Path(__file__).resolve().parent.parent 
    countries = {'HTI': '75N180W', 'GHA': '75N060W', 'MOZ': '00N060W', 'NAM': '00N060W', 'NPL': '75N060E'}
    extents = list(set([x for _, x in countries.items()]))
    years = list(range(2013, 2018))
    for year in years:
        YEARDIR = BASEDIR.joinpath(f'datain/{year}')
        if not YEARDIR.exists():
            YEARDIR.mkdir()
        urls = get_urls.GetNOOAUrls(years=year, extent=extents, annual_composites=True)
        with ThreadPoolExecutor(max_workers=2) as executor:
            for i in urls.hrefs:
                executor.submit(main, i, year)