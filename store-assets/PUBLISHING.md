# Публикация Pulse в Google Play и Microsoft Store

Pulse — это веб-приложение (PWA), поэтому в магазины оно попадает через «упаковку»:
- **Google Play** → Trusted Web Activity (TWA) / Bubblewrap. Это официальный способ
  (им так выпущены Twitter, Trivago и др.). Приложение = тонкая обёртка Chrome над вашим сайтом.
- **Microsoft Store** → два пути: PWA Builder (упаковка в MSIX, самый быстрый) или
  WebView2-обёртка (если нужны нативные фичи).

## 0. Обязательные предварительные условия
1. **HTTPS на постоянном домене.** Render free-домен `*.onrender.com` подходит технически,
   но лучше купить свой домен (магазин проверяет совпадение домена и SHA-256 отпечатка
   файла `/.well-known/assetlinks.json`).
2. **PWA уже должен устанавливаться без ошибок**: проверьте в Chrome DevTools →
   Application → Manifest, и Lighthouse → PWA. В проекте уже настроены:
   manifest.json (id, display_override, maskable-иконки), sw.js, push-уведомления.
3. Аккаунты: Google Play Console — $25 (разово), Microsoft Partner Center — $19 (разово).

## 1. Google Play (TWA через Bubblewrap)
### Шаг 1. Сгенерировать Digital Asset Links
```bash
npx @bubblewrap/cli doctor          # установит JDK/Ant/Android SDK при необходимости
cd store-assets
npx @bubblewrap/cli generate-signing-key-report   # создаёт pulse.keystore + assetlinks.json
```
Храните `pulse.keystore` и пароль **вечности** (без него нельзя обновлять приложение!).

### Шаг 2. Загрузить assetlinks.json на сервер
Файл из шага 1 положите по адресу `https://ваш-домен/.well-known/assetlinks.json`
(в проекте можно отдать его статикой из `public/.well-known/assetlinks.json`,
Content-Type: application/json). Без этого файла TWA покажет белый экран!

### Шаг 3. Собрать проект
```bash
npx @bubblewrap/cli init --manifest https://ваш-домен/manifest.json
# отвечает на вопросы: package_name=com.pulse.messenger, host=ваш-домен, signing=из шага 1
npx @bubblewrap/cli build          # -> app/release/app-release-signed.aab
```
Готовые значения лежат в `store-assets/trusted-web-activity.json`
(не забудьте заменить URL на реальный домен!).

### Шаг 4. Консоль Play
1. play.google.com/console → создать приложение «Pulse».
2. Заполнить анкеты: контент-рейтинг, политики конфиденциальности
   (мессенджер собирает email/пароли — обязательна ссылка на privacy policy!),
   целевая аудитория, «приложение использует интернет».
3. Тестирование → Internal testing → загрузить `.aab`.
4. Материалы магазина: иконка 512x512 (`icon-app-512.png`), feature graphic 1024x500
   (`feature-graphic-1024x500.png`), скриншоты (замените заглушку реальными!),
   короткое (<80 симв.) и полное описания.
5. После 12+ тестировщиков 14 дней (для новых аккаунтов) → production.

## 2. Microsoft Store (Windows)
### Вариант А — PWA Builder (рекомендую, ~1 час)
1. Откройте https://www.pwabuilder.com → введите URL вашего сайта.
2. Evaluate → Package → **Generate Package** → Windows (MSIX).
   Скачается пакет с иконками/плитками, сгенерированными из манифеста.
3. partner.microsoft.com → зарегистрироваться ($19) → Create new app →
   вбить точное имя продукта из пакета → Products for Windows → загрузить MSIX.
4. Заполнить метаданные (скриншоты, описание, рейтинг ESRB) → Submit for certification.

### Вариант Б — WebView2 + Visual Studio (если нужен офлайн-режим/нативные API)
1. VS 2022 → проект «Packaged app for Desktop in C#» (или Electron + electron-builder target `appx`).
2. Внутри — контрол WebView2, указывающий на https://ваш-домен.
3. Manifest: добавить Capability `internetClient`.
4. Сборка MSIX → тот же Partner Center.

## 3. Чек-лист перед отправкой в оба магазина
- [ ] Рабочая ссылка на Privacy Policy (требование обеих площадок для мессенджеров)
- [ ] HTTPS без смешанного контента (wss:// для Socket.IO уже ок)
- [ ] Иконки: regular + maskable (готово: public/icon-*.png, store-assets/icon-*.png)
- [ ] Реальные скриншоты вместо заглушек (минимум 2 на магазин)
- [ ] app-release-signed.aab собран и ставится на тестовое устройство
- [ ] assetlinks.json доступен по /.well-known/ и совпадает с ключом подписи
- [ ] На бесплатном тарифе Render сервис «засыпает» — для магазинов это минус к
      отзывам; рассмотрите платный тариф или собственный VPS.

## 4. iOS (App Store) — если понадобится позже
Apple **запрещает** чистые веб-обёртки (правило 4.5). Для мессенджера потребуется
нативная оболочка (Capacitor) + доработка UI под Human Interface Guidelines, либо
публикация как Safari Web App (добавление на домашний экран) без App Store.
В проекте уже есть meta apple-mobile-web-app-*.
