from pathlib import Path
import get_urls
import download_viirs
from concurrent.futures import ThreadPoolExecutor
import time

def main(url, year):
    #Download a tile
    download = download_viirs.DownloadViirs(url)
    download.download_rasters()
    #Unzip the tile
    download.open_tgz()
    #Check if it's nam and moz
    if '00N060W' in url:
        #extract cvg and rad for nam and moz

        
        print('Doing Moz and Nam')
    else:
        #extract cvg and rad for others

    #delete the tile
    print(year)
    
    time.sleep(0.2)



if __name__ == "__main__":
    BASEDIR = Path(__file__).resolve().parent.parent 
    countries = {'HTI': '75N180W', 'GHA': '75N060W', 'MOZ': '00N060W', 'NAM': '00N060W', 'NPL': '75N060E'}
    extents = list(set([x for _, x in countries.items()]))
    print(len(extents))
    years = list(range(2013, 2018))
    for year in years:
        YEARDIR = BASEDIR.joinpath(f'datain/{year}')
        if not YEARDIR.exists():
            YEARDIR.mkdir()
        urls = get_urls.GetNOOAUrls(years=year, extent=extents, annual_composites=True)
        print(len(urls.hrefs))
        with ThreadPoolExecutor(max_workers=5) as executor:
            for i in urls.hrefs:
                executor.submit(main, i, year)