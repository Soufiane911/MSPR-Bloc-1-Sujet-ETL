import { expect, test } from "@playwright/test";

const SAME_ORIGIN_API = /http:\/\/(?:localhost|127\.0\.0\.1):\d+\/api\/.*/;
const DIRECT_API = /http:\/\/(?:localhost|127\.0\.0\.1):8000\/.*/;

async function fulfillJson(route, status, body) {
  await route.fulfill({
    status,
    contentType: "application/json",
    body: JSON.stringify(body),
  });
}

async function mockDashboardApi(route) {
  const url = new URL(route.request().url());
  const pathname = url.pathname.replace(/^\/api/, "");

  const responses = {
    "/stats/summary": { trains: 1, stations: 2, schedules: 1 },
    "/stats/by-country": [{ country: "FR", nb_trains: 1 }],
    "/stats/day-night": [{ country: "FR", train_type: "day", nb_trains: 1, nb_schedules: 1 }],
    "/stats/top-routes": [],
    "/stats/data-quality": [{ table_name: "schedules", total_records: 1 }],
    "/trains/": [{ train_id: 1, train_type: "day" }],
    "/stations/": [
      { station_id: 1, name: "Paris Gare de Lyon", country: "FR" },
      { station_id: 2, name: "Lyon Part-Dieu", country: "FR" },
    ],
    "/operators/": [{ operator_id: 1, name: "SNCF", country: "FR" }],
    "/schedules/": [{ schedule_id: 1, distance_km: 465 }],
    "/trajets": [
      {
        trajet_id: 1,
        train_number: "TGV-1",
        operator_name: "SNCF",
        train_type: "day",
        origin: "Paris Gare de Lyon",
        destination: "Lyon Part-Dieu",
        origin_country: "FR",
        destination_country: "FR",
        distance_km: 465,
        duration_min: 120,
      },
    ],
  };

  if (pathname.startsWith("/aviationStats")) {
    await fulfillJson(route, 503, { detail: "Aviation unavailable" });
    return;
  }

  if (Object.prototype.hasOwnProperty.call(responses, pathname)) {
    await fulfillJson(route, 200, responses[pathname]);
    return;
  }

  await fulfillJson(route, 404, { detail: `Unexpected route: ${pathname}` });
}

test("les appels API passent par le proxy meme origine de la page", async ({ page }) => {
  const directApiCalls = [];

  await page.route(DIRECT_API, async (route) => {
    directApiCalls.push(route.request().url());
    await route.abort("failed");
  });
  await page.route(SAME_ORIGIN_API, mockDashboardApi);

  await page.goto("/");

  await expect(page.getByText("Chargement des donnees...")).toBeHidden({ timeout: 30000 });
  await expect(page.getByText(/^Erreur:/)).toHaveCount(0);
  await expect(page.getByText("Trains", { exact: true })).toBeVisible();
  expect(directApiCalls).toEqual([]);
});
