import { MapContainer, Marker, Popup, TileLayer } from "react-leaflet";
import L from "leaflet";

delete L.Icon.Default.prototype._getIconUrl;
L.Icon.Default.mergeOptions({
  iconRetinaUrl: "https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon-2x.png",
  iconUrl: "https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon.png",
  shadowUrl: "https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png",
});

export default function MapTab({ stations }) {
  const points = (stations || []).filter((s) => s.latitude && s.longitude).slice(0, 1500);

  return (
    <section className="card">
      <h3>Carte des gares</h3>
      <p className="muted">{points.length} gares affichees (echantillon)</p>
      <div className="map-wrap">
        <MapContainer center={[50, 10]} zoom={4} scrollWheelZoom={false}>
          <TileLayer
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          />
          {points.map((station) => (
            <Marker
              key={station.station_id}
              position={[station.latitude, station.longitude]}
            >
              <Popup>
                <strong>{station.name}</strong>
                <br />
                {station.city || ""} {station.country || ""}
              </Popup>
            </Marker>
          ))}
        </MapContainer>
      </div>
    </section>
  );
}
