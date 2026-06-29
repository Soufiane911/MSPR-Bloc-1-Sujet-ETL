import { expect, test } from "@playwright/test";

const API_URL = /http:\/\/(?:localhost|127\.0\.0\.1):8000\/.*/;

async function fulfillJson(route, status, body) {
  await route.fulfill({
    status,
    contentType: "application/json",
    body: JSON.stringify(body),
  });
}

async function mockDashboardApi(page) {
  await page.route(API_URL, async (route) => {
    const url = new URL(route.request().url());

    if (url.pathname.startsWith("/aviationStats")) {
      await fulfillJson(route, 503, { detail: "Aviation unavailable" });
      return;
    }

    const responses = {
      "/stats/summary": { trains: 1, stations: 2, schedules: 1 },
      "/stats/by-country": [{ country: "FR", nb_trains: 1 }],
      "/stats/day-night": [{ country: "FR", train_type: "day", nb_trains: 1, nb_schedules: 1 }],
      "/stats/top-routes": [
        {
          origin_city: "Paris",
          destination_city: "Lyon",
          frequency: 4,
          avg_duration: 120,
          avg_distance: 465,
        },
      ],
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

    if (Object.prototype.hasOwnProperty.call(responses, url.pathname)) {
      await fulfillJson(route, 200, responses[url.pathname]);
      return;
    }

    await fulfillJson(route, 404, { detail: `Unexpected route: ${url.pathname}` });
  });
}

test("le dashboard ferroviaire reste utilisable si les stats aviation sont indisponibles", async ({ page }) => {
  await mockDashboardApi(page);

  await page.goto("/");

  await expect(page.getByText("Chargement des donnees...")).toBeHidden({ timeout: 30000 });
  await expect(page.getByText(/^Erreur:/)).toHaveCount(0);
  await expect(page.getByText("Trains", { exact: true })).toBeVisible();

  await page.getByRole("button", { name: "Trajets", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Consultation des trajets" })).toBeVisible();
  await expect(page.getByText("Paris Gare de Lyon")).toBeVisible();
});
