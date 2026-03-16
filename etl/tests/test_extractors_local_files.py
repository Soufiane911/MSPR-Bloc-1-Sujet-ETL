import json

from extractors.back_on_track import BackOnTrackExtractor
from extractors.gtfs_extractor import GTFSExtractor
from extractors.mobility_catalog import MobilityCatalogExtractor


def test_gtfs_extractor_reads_local_files(tmp_path):
    raw_dir = tmp_path / "data" / "raw" / "sncf_transilien"
    raw_dir.mkdir(parents=True)
    (raw_dir / "agency.txt").write_text(
        "agency_id,agency_name\n1,SNCF\n", encoding="utf-8"
    )
    (raw_dir / "stops.txt").write_text(
        "stop_id,stop_name\nPAR,Paris\n", encoding="utf-8"
    )
    (raw_dir / "routes.txt").write_text(
        "route_id,route_short_name\nR1,TGV\n", encoding="utf-8"
    )
    (raw_dir / "trips.txt").write_text(
        "route_id,service_id,trip_id\nR1,S1,T1\n", encoding="utf-8"
    )
    (raw_dir / "stop_times.txt").write_text(
        "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
        "T1,08:00:00,08:00:00,PAR,1\n",
        encoding="utf-8",
    )

    extractor = GTFSExtractor(source_name="sncf_transilien", country_code="FR")
    extractor.extract_path = raw_dir

    data = extractor.extract()

    assert sorted(data.keys()) == ["agency", "routes", "stop_times", "stops", "trips"]
    assert extractor.validate() is True


def test_gtfs_extractor_strips_headers_and_skips_optional_files_by_default(tmp_path):
    raw_dir = tmp_path / "data" / "raw" / "renfe"
    raw_dir.mkdir(parents=True)
    (raw_dir / "agency.txt").write_text(
        "agency_id,agency_name,agency_phone   \n1,Renfe,123\n", encoding="utf-8"
    )
    (raw_dir / "stops.txt").write_text(
        "stop_id,stop_name   ,unused_col\nMAD,Madrid,x\n", encoding="utf-8"
    )
    (raw_dir / "routes.txt").write_text(
        "route_id,route_short_name,route_type\nR1,AVE,2\n", encoding="utf-8"
    )
    (raw_dir / "trips.txt").write_text(
        "route_id,service_id,trip_id\nR1,S1,T1\n", encoding="utf-8"
    )
    (raw_dir / "stop_times.txt").write_text(
        "trip_id,arrival_time,departure_time,stop_id,stop_sequence,extra\n"
        "T1,08:00:00,08:00:00,MAD,1,x\n",
        encoding="utf-8",
    )
    (raw_dir / "feed_info.txt").write_text(
        "feed_publisher_name,feed_publisher_url,feed_lang\nRenfe,https://renfe.test,es\n",
        encoding="utf-8",
    )

    extractor = GTFSExtractor(source_name="renfe", country_code="ES")
    extractor.extract_path = raw_dir

    data = extractor.extract()

    assert "feed_info" not in data
    assert list(data["stops"].columns) == ["stop_id", "stop_name"]
    assert list(data["agency"].columns) == ["agency_id", "agency_name", "agency_phone"]
    assert extractor.validate() is True


def test_back_on_track_extractor_reads_local_json_files(tmp_path):
    raw_dir = tmp_path / "data" / "raw" / "back_on_track"
    raw_dir.mkdir(parents=True)

    payloads = {
        "agencies": {"agency_1": {"agency_id": "A1", "agency_name": "Nightjet"}},
        "stops": {"stop_1": {"stop_id": "PAR", "stop_name": "Paris"}},
        "routes": {"route_1": {"route_id": "R1", "agency_id": "A1"}},
        "trips": {"trip_1": {"trip_id": "T1", "route_id": "R1"}},
        "trip_stop": {"ts_1": {"trip_id": "T1", "stop_id": "PAR"}},
    }

    for filename, payload in payloads.items():
        (raw_dir / f"{filename}.json").write_text(json.dumps(payload), encoding="utf-8")

    extractor = BackOnTrackExtractor()
    extractor.local_path = raw_dir

    data = extractor.extract()

    assert set(payloads).issubset(data["catalog"].keys())
    assert extractor.validate() is True


def test_mobility_catalog_extractor_reads_local_csv(tmp_path):
    raw_dir = tmp_path / "data" / "raw" / "mobility_catalog"
    raw_dir.mkdir(parents=True)
    (raw_dir / "data.csv").write_text(
        "mdb_source_id,data_type,provider,urls.direct_download,location.country_code\n"
        "1,gtfs,SNCF,https://example.test/feed.zip,FR\n",
        encoding="utf-8",
    )

    extractor = MobilityCatalogExtractor()
    extractor.local_path = raw_dir / "data.csv"

    data = extractor.extract()

    assert len(data["catalog"]) == 1
    assert extractor.validate() is True
