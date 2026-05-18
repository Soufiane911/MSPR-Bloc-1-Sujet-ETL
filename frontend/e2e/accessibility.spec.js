import { expect, test } from "@playwright/test";

/**
 * Tests d'accessibilite basiques (RGAA / WCAG 2.1 niveau A)
 * Verifie : langue, skip link, focus visible, contrastes, roles ARIA.
 */

test("la page declare la langue francaise", async ({ page }) => {
  await page.goto("/");
  const html = page.locator("html");
  await expect(html).toHaveAttribute("lang", "fr");
});

test("un lien 'Aller au contenu' est present et focusable", async ({ page }) => {
  await page.goto("/");
  const skipLink = page.locator(".skip-link");

  // Le lien existe dans le DOM
  await expect(skipLink).toHaveCount(1);
  await expect(skipLink).toHaveText("Aller au contenu");
  await expect(skipLink).toHaveAttribute("href", "#contenu-principal");

  // Au focus clavier (Tab), le lien devient visible (top passe de -40px a 0)
  await page.keyboard.press("Tab");
  const top = await skipLink.evaluate((el) => window.getComputedStyle(el).top);
  expect(top).toBe("0px");
});

test("le focus est visible sur les boutons et liens", async ({ page }) => {
  await page.goto("/");

  // Attendre le chargement des donnees
  await expect(page.getByText("Chargement des donnees...")).toBeHidden({ timeout: 30000 });

  // Focus sur un bouton d'onglet et verifier l'outline
  const firstTab = page.locator(".tabs button").first();
  await firstTab.focus();

  const outline = await firstTab.evaluate((el) => window.getComputedStyle(el).outlineWidth);
  expect(parseFloat(outline)).toBeGreaterThan(0);
});

test("le contenu principal possede un id et un role approprie", async ({ page }) => {
  await page.goto("/");

  const main = page.locator("main#contenu-principal");
  await expect(main).toHaveCount(1);
  await expect(main).toHaveAttribute("role", "main");
});

test("les textes principaux ont un contraste suffisant", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByText("Chargement des donnees...")).toBeHidden({ timeout: 30000 });

  // Verifier la couleur du texte principal (body)
  const bodyColor = await page.evaluate(() => window.getComputedStyle(document.body).color);
  const bodyBg = await page.evaluate(() => window.getComputedStyle(document.body).backgroundColor);

  // Couleurs attendues : texte sombre (#1f2a38) sur fond clair (#f6f4ec)
  expect(bodyColor).toMatch(/rgb\(31,\s*42,\s*56\)/);
  expect(bodyBg).toMatch(/rgb\(246,\s*244,\s*236\)/);
});

test("les images possedent un attribut alt ou un role de presentation", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByText("Chargement des donnees...")).toBeHidden({ timeout: 30000 });

  const images = page.locator("img");
  const count = await images.count();

  for (let i = 0; i < count; i++) {
    const img = images.nth(i);
    const alt = await img.getAttribute("alt");
    const role = await img.getAttribute("role");
    // Soit un alt est present, soit role="presentation"
    expect(alt !== null || role === "presentation").toBe(true);
  }
});
