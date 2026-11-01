# Serveur Scaleway éphémère — plan d'implémentation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** exécuter `decp_processing` et `sirene_preprocess` (prod et dev) sur une instance Scaleway MEMORY3-X8C-64G créée pour le run puis détruite, orchestrée depuis le serveur Prefect actuel.

**Architecture:** un flow lanceur (pool `local`) acquiert la limite `volume-<env>`, nettoie les orphelins, crée l'instance (volume de données attaché, cloud-init qui démarre un worker sur `scaleway-<env>`), déclenche le déploiement `*-exec-<env>`, attend son état final, rapatrie `dist/` par rsync, bascule `dist/` atomiquement, puis détruit l'instance dans un `finally`. La logique du lanceur est une fonction pure à dépendances injectées ; Scaleway, Prefect et rsync sont branchés par le flow.

**Tech Stack:** Python ≥ 3.11, Prefect 3.9.1, SDK `scaleway` 2.12 (`InstanceUtilsV1API`, `BlockV1Alpha1API`), cloud-init, uv, rsync/ssh, pytest.

**Spec:** `docs/superpowers/specs/2026-10-31-serveur-ephemere-scaleway-design.md`

## Global Constraints

- Prefect `3.9.1` partout (`prefect==3.9.1`, `prefect[email]==3.9.1`) ; `requires-python = ">= 3.11"` (Prefect 3.9 ne supporte plus 3.10).
- Instance par défaut : `MEMORY3-X8C-64G`, Debian 12 (`debian_bookworm`), disque système 40 Go, IPv4 dynamique.
- Volumes de données : Block Storage 5K, 30 Go, noms `decp-data-prod` et `decp-data-dev`, montés sur `/data`.
- Pools : `scaleway-prod`, `scaleway-dev` (type `process`). Limites de concurrence globales : `volume-prod`, `volume-dev`, valeur 1.
- Tags d'instance : `decp-processing`, `env:<env>`, `flow-run:<id du run lanceur>`.
- Variables sur l'instance : `DECP_DATA_DIR=/data`, `PREFECT_HOME=/data/.prefect`, `DECP_DIST_DIR=/work/dist`, `PREFECT_API_URL`, `PREFECT_API_AUTH_STRING`, `DATAGOUVFR_API_KEY`, `S3_*`.
- Délais par défaut : `start_timeout_minutes=20`, `max_duration_hours=4` ; création d'instance et rsync : 2 retries.
- Secrets (`DATAGOUVFR_API_KEY`, `S3_SECRET_ACCESS_KEY`, `PREFECT_API_AUTH_STRING`, clés Scaleway) : jamais dans les logs ni dans les `job_variables` des déploiements.
- Clé API Scaleway uniquement dans le `.env` du serveur actuel : `SCW_ACCESS_KEY`, `SCW_SECRET_KEY`, `SCW_DEFAULT_PROJECT_ID`, `SCW_DEFAULT_ZONE` (défaut `fr-par-1`).
- Langue : code, logs et docs en français, comme le reste du dépôt.

## Review Focus

1. **Volume encore attaché à une instance orpheline** : la création échoue si le volume est pris. Attendu : les orphelins de l'environnement sont détruits avant la création → test `test_orphelins_de_l_env_detruits_avant_creation` (Task 4).
2. **Premier démarrage puis démarrages suivants** : le cloud-init ne doit formater le volume que s'il est vierge, jamais un volume qui contient déjà des données → assertion sur la garde `blkid` dans `test_user_data_ne_formate_que_si_vierge` (Task 2).
3. **Destruction de l'instance qui emporterait le volume de données** : `TERMINATE` supprime les volumes attachés. Attendu : détachement du volume de données avant `TERMINATE` → ordre vérifié par `test_delete_instance_detache_avant_terminate` (Task 3).
4. **Fuite de secrets dans les logs Prefect** : le user-data contient les secrets. Attendu : aucun secret dans les logs du lanceur → `test_aucun_secret_dans_les_logs` (Task 4).
5. **Redémarrage du serveur Prefect pendant un run de 4 h** : le renouvellement du bail de la limite de concurrence échoue. Attendu : le run continue et l'instance est quand même détruite → `concurrency(..., raise_on_lease_renewal_failure=False)` (Task 5), vérifié en rollout (Task 7).

---

### Task 1: Prefect 3.9.1, SDK Scaleway, Python ≥ 3.11

**Files:**

- Modify: `pyproject.toml:5,14-15,7-27`
- Modify: `uv.lock` (régénéré)

**Interfaces:**

- Produces: environnement avec `prefect==3.9.1` et `scaleway` importables.

- [ ] **Step 1: Modifier `pyproject.toml`**

`requires-python = ">= 3.11"`, `prefect==3.9.1`, `prefect[email]==3.9.1`, ajouter `"scaleway"` aux dépendances.

- [ ] **Step 2: Régénérer le lock et synchroniser**

Run: `uv lock && uv sync`
Expected: `prefect==3.9.1` et `scaleway` dans `uv.lock`.

- [ ] **Step 3: Vérifier l'API de déploiement utilisée par `src/deployments.py`**

Run: `uv run python -c "from prefect import flow; from prefect.runner.storage import GitRepository; import inspect; print(inspect.signature(flow.from_source)); f=flow.from_source(source=GitRepository(url='https://github.com/ColinMaudry/decp-processing.git', branch='dev'), entrypoint='src/flows/decp_processing.py:decp_processing'); print(inspect.signature(f.deploy))"`
Expected: les deux signatures s'affichent, `deploy` accepte `name`, `work_pool_name`, `cron`, `job_variables`, `ignore_warnings`. Si `from_source` a été retiré avec les « deprecated Runner execution paths » de 3.9.0, s'arrêter et remonter l'écart avant toute autre tâche.

- [ ] **Step 4: Lancer la suite de tests**

Run: `uv run pytest -q`
Expected: tous les tests passent (117 avant ce plan).

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "Prefect 3.9.1, SDK Scaleway, Python >= 3.11 (#189)"
```

---

### Task 2: `swap_dist` et rendu du cloud-init

**Files:**

- Modify: `src/tasks/output.py` (ajout de `swap_dist`)
- Modify: `src/flows/decp_processing.py:177-183` (utiliser `swap_dist`)
- Create: `src/infra/__init__.py`
- Create: `src/infra/cloud-init.sh`
- Create: `src/infra/user_data.py`
- Test: `tests/test_output.py`, `tests/test_user_data.py`

**Interfaces:**

- Produces: `swap_dist(work_dir: Path, dist_dir: Path) -> None` (`src/tasks/output.py`)
- Produces: `render_user_data(*, env: str, branch: str, volume_id: str, ssh_public_key: str, env_vars: dict[str, str]) -> str` (`src/infra/user_data.py`)

- [ ] **Step 1: Écrire les tests de `swap_dist`** (`tests/test_output.py`)

```python
def test_swap_dist_remplace_l_existant(tmp_path):
    dist, wip = tmp_path / "dist", tmp_path / "dist_wip"
    dist.mkdir(); (dist / "ancien.txt").write_text("a")
    wip.mkdir(); (wip / "nouveau.txt").write_text("n")
    swap_dist(wip, dist)
    assert sorted(p.name for p in dist.iterdir()) == ["nouveau.txt"]
    assert not wip.exists() and not (tmp_path / "dist_old").exists()

def test_swap_dist_premier_run(tmp_path):
    wip = tmp_path / "dist_wip"; wip.mkdir(); (wip / "f").write_text("x")
    swap_dist(wip, tmp_path / "dist")
    assert (tmp_path / "dist" / "f").read_text() == "x"

def test_swap_dist_sans_wip_ne_touche_rien(tmp_path):
    dist = tmp_path / "dist"; dist.mkdir(); (dist / "f").write_text("x")
    with pytest.raises(FileNotFoundError):
        swap_dist(tmp_path / "dist_wip", dist)
    assert (dist / "f").read_text() == "x"
```

- [ ] **Step 2: Vérifier l'échec**

Run: `uv run pytest tests/test_output.py -k swap_dist -v`
Expected: FAIL (`swap_dist` non importable).

- [ ] **Step 3: Implémenter `swap_dist` et l'utiliser dans `decp_processing`**

Reprendre la logique de `src/flows/decp_processing.py:177-183` (`dist` → `<nom>_old`, `wip` → `dist`, suppression de `_old`), précédée d'un `FileNotFoundError` si `work_dir` n'existe pas. Remplacer le bloc du flow par `swap_dist(work_dir, DIST_DIR)`.

- [ ] **Step 4: Écrire les tests du user-data** (`tests/test_user_data.py`)

```python
ARGS = dict(env="prod", branch="main", volume_id="11111111-2222-3333-4444-555555555555",
            ssh_public_key="ssh-ed25519 AAAAtest decp-launcher",
            env_vars={"DECP_DATA_DIR": "/data", "DATAGOUVFR_API_KEY": "secret-123"})

def test_user_data_contient_branche_pool_volume_et_cle():
    ud = render_user_data(**ARGS)
    assert ud.startswith("#!/bin/bash")
    assert "--branch main" in ud
    assert "--pool scaleway-prod" in ud
    assert "11111111-2222-3333-4444-555555555555" in ud
    assert "ssh-ed25519 AAAAtest decp-launcher" in ud
    assert "uv sync --frozen" in ud

def test_user_data_ecrit_les_variables_en_fichier_600():
    ud = render_user_data(**ARGS)
    assert "DATAGOUVFR_API_KEY='secret-123'" in ud
    assert "chmod 600 /etc/decp.env" in ud

def test_user_data_ne_formate_que_si_vierge():
    ud = render_user_data(**ARGS)
    assert "blkid" in ud
    assert ud.index("blkid") < ud.index("mkfs.ext4")

def test_user_data_refuse_une_valeur_avec_quote():
    with pytest.raises(ValueError):
        render_user_data(**{**ARGS, "env_vars": {"X": "a'b"}})
```

- [ ] **Step 5: Vérifier l'échec**

Run: `uv run pytest tests/test_user_data.py -v`
Expected: FAIL (module absent).

- [ ] **Step 6: Écrire `src/infra/cloud-init.sh` et `render_user_data`**

Modèle `string.Template` (placeholders `${ENV}`, `${BRANCH}`, `${VOLUME_ID}`, `${SSH_PUBLIC_KEY}`, `${ENV_FILE}`). Le user-data est un script bash (`#!/bin/bash`, `set -euxo pipefail`), exécuté une fois par cloud-init. `render_user_data` lève `ValueError` si une valeur contient `'` ou un saut de ligne, puis produit `${ENV_FILE}` sous forme de lignes `CLE='valeur'`.

```bash
#!/bin/bash
set -euxo pipefail
DEV=$(ls /dev/disk/by-id/*${VOLUME_ID}* | head -1)
blkid "$DEV" || mkfs.ext4 -L decp-data "$DEV"
mkdir -p /data /work
echo "$DEV /data ext4 defaults,nofail 0 2" >> /etc/fstab
mount /data
install -d -m 700 /root/.ssh
echo '${SSH_PUBLIC_KEY}' >> /root/.ssh/authorized_keys
cat > /etc/decp.env <<'EOF'
${ENV_FILE}
EOF
chmod 600 /etc/decp.env
apt-get update && apt-get install -y git rsync curl
curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR=/usr/local/bin sh
git clone --depth 1 --branch ${BRANCH} https://github.com/ColinMaudry/decp-processing.git /opt/decp-processing
cd /opt/decp-processing && uv sync --frozen
cat > /etc/systemd/system/prefect-worker.service <<'EOF'
[Service]
EnvironmentFile=/etc/decp.env
WorkingDirectory=/opt/decp-processing
ExecStart=/opt/decp-processing/.venv/bin/prefect worker start --pool scaleway-${ENV}
Restart=always
[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload && systemctl enable --now prefect-worker
```

- [ ] **Step 7: Vérifier**

Run: `uv run pytest tests/test_output.py tests/test_user_data.py -v && bash -n <(uv run python -c "from src.infra.user_data import render_user_data as r; print(r(env='dev', branch='dev', volume_id='v', ssh_public_key='k', env_vars={'A': 'b'}))")`
Expected: PASS, et `bash -n` sans erreur de syntaxe.

- [ ] **Step 8: Commit**

```bash
git add src/tasks/output.py src/flows/decp_processing.py src/infra tests/test_output.py tests/test_user_data.py
git commit -m "swap_dist et user-data de l'instance éphémère (#189)"
```

---

### Task 3: Couche Scaleway (`src/infra/scaleway.py`)

**Files:**

- Create: `src/infra/scaleway.py`
- Test: `tests/test_scaleway.py`

**Interfaces:**

- Consumes: SDK `scaleway` (`Client.from_config_file_and_env()`, `InstanceUtilsV1API`, `BlockV1Alpha1API`).
- Produces:

  - `@dataclass(frozen=True) class Instance: id: str; ip: str | None; tags: list[str]; created_at: datetime` (tz-aware UTC)
  - `class ScalewayProvider` avec :
    - `create_instance(*, name: str, instance_type: str, data_volume_id: str, user_data: str, tags: list[str]) -> Instance` : volumes `{"0": boot sbs 40 Go nommé name+"-boot", "1": data_volume_id}`, `dynamic_ip_required=True`, `image="debian_bookworm"`, `set_server_user_data(key="cloud-init")`, `POWERON`, puis `wait_instance_server` jusqu'à `running`.
    - `delete_instance(instance_id: str, data_volume_id: str) -> None` : `POWEROFF` + attente `stopped`, `detach_server_volume(data_volume_id)`, `TERMINATE`, puis suppression du volume `<name>-boot` s'il subsiste (Block API, recherche par nom).
    - `list_instances(tags: list[str]) -> list[Instance]`
    - `volume_id(name: str) -> str` (Block API `list_volumes(name=...)`, `LookupError` si absent).

- [ ] **Step 1: Écrire le test d'ordre de destruction** (`tests/test_scaleway.py`)

```python
def test_delete_instance_detache_avant_terminate():
    api = MagicMock()
    p = ScalewayProvider(instance_api=api, block_api=MagicMock(), zone="fr-par-1")
    p.delete_instance("srv", "vol-data")
    calls = [c[0] for c in api.method_calls]
    assert calls.index("detach_server_volume") < [i for i, c in enumerate(api.method_calls)
        if c[0] == "server_action" and c.kwargs["action"] == ServerAction.TERMINATE][0]
    api.detach_server_volume.assert_called_once_with(server_id="srv", volume_id="vol-data", zone="fr-par-1")
```

`ScalewayProvider.__init__(self, instance_api=None, block_api=None, zone=None)` construit les API depuis `Client.from_config_file_and_env()` quand elles ne sont pas fournies.

- [ ] **Step 2: Vérifier l'échec**

Run: `uv run pytest tests/test_scaleway.py -v`
Expected: FAIL (module absent).

- [ ] **Step 3: Implémenter `ScalewayProvider`** selon l'interface ci-dessus. Aucune logique Prefect, aucun log du user-data.

- [ ] **Step 4: Vérifier**

Run: `uv run pytest tests/test_scaleway.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/infra/scaleway.py tests/test_scaleway.py
git commit -m "Couche Scaleway : création et destruction d'instance (#189)"
```

---

### Task 4: Logique du lanceur (fonction pure)

**Files:**

- Create: `src/infra/remote_run.py`
- Test: `tests/test_remote_run.py`

**Interfaces:**

- Consumes: `Instance` (Task 3), `swap_dist` (Task 2).
- Produces:

  - `class Provider(Protocol)` : `create_instance`, `delete_instance`, `list_instances` (signatures de Task 3).
  - `class RunState(Enum): PENDING, RUNNING, COMPLETED, FAILED` (PENDING = planifié ou en attente de worker).
  - `@dataclass class RemoteRunConfig: env: str; deployment: str; launcher_run_id: str; instance_type: str; data_volume_id: str; user_data: str; dist_dir: Path | None; start_timeout: timedelta; max_duration: timedelta; poll_interval: timedelta = timedelta(seconds=30)`
  - `execute_remote_run(cfg: RemoteRunConfig, *, provider: Provider, trigger: Callable[[], str], get_state: Callable[[str], RunState], cancel: Callable[[str], None], sync_dist: Callable[[str, Path], None], fetch_boot_log: Callable[[str], str], now: Callable[[], datetime], sleep: Callable[[float], None], logger) -> None` : lève `RemoteRunError(message)` en cas d'échec. Ordre : `cleanup_orphans` → `create_instance` → `trigger` → boucle d'attente → si `COMPLETED` et `dist_dir` : `sync_dist(ip, Path(f"{dist_dir}_wip"))` puis `swap_dist` → `finally: delete_instance`.
  - `cleanup_orphans(provider, env: str, now: datetime, max_age: timedelta, data_volume_ids: dict[str, str]) -> list[str]` (ids détruits).

- [ ] **Step 1: Écrire les tests** (`tests/test_remote_run.py`), avec un `FakeProvider` qui enregistre les appels et des `get_state` scriptés :

```python
def test_succes_rapatrie_bascule_et_detruit(tmp_path): ...
    # états PENDING, RUNNING, COMPLETED ; dist_dir=tmp_path/"dist"
    # sync_dist appelé une fois avec (ip, tmp_path/"dist_wip") ; dist bascule ; delete_instance appelé une fois

def test_echec_du_run_detruit_sans_rapatrier(): ...
    # COMPLETED jamais atteint, FAILED → RemoteRunError, sync_dist non appelé, delete_instance appelé

def test_delai_de_demarrage_depasse_annule_logue_et_detruit(): ...
    # PENDING au-delà de start_timeout → cancel(run_id), fetch_boot_log appelé, message dans les logs, RemoteRunError, delete_instance appelé

def test_duree_maximale_depassee_annule_et_detruit(): ...
    # RUNNING au-delà de max_duration → cancel, RemoteRunError, delete_instance

def test_echec_de_creation_ne_declenche_rien(): ...
    # create_instance lève → trigger non appelé, delete_instance non appelé, exception propagée

def test_echec_du_rsync_ne_bascule_pas(tmp_path): ...
    # sync_dist lève → dist inchangé, RemoteRunError, delete_instance appelé

def test_sans_dist_dir_pas_de_rsync(): ...
    # dist_dir=None (sirene) → sync_dist non appelé

def test_orphelins_de_l_env_detruits_avant_creation(): ...
    # instance env:prod existante → delete_instance avant create_instance

def test_orphelin_d_un_autre_env_trop_ancien_detruit(): ...
    # env:dev créée il y a 5 h, max_age 4 h → détruite ; env:dev créée il y a 1 h → conservée

def test_aucun_secret_dans_les_logs(caplog): ...
    # user_data contenant "secret-123" → aucune ligne de log ne contient "secret-123", même en échec
```

L'horloge est simulée : `now()` avance de `poll_interval` à chaque `sleep()`.

- [ ] **Step 2: Vérifier l'échec**

Run: `uv run pytest tests/test_remote_run.py -v`
Expected: FAIL (module absent).

- [ ] **Step 3: Implémenter `execute_remote_run` et `cleanup_orphans`** selon l'interface. Le délai de démarrage est mesuré depuis `trigger()` tant que l'état est `PENDING` ; la durée maximale, depuis le premier `RUNNING`.

- [ ] **Step 4: Vérifier**

Run: `uv run pytest tests/test_remote_run.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/infra/remote_run.py tests/test_remote_run.py
git commit -m "Logique du lanceur : cycle de vie de l'instance et du run distant (#189)"
```

---

### Task 5: Flow lanceur `run_on_scaleway`

**Files:**

- Create: `src/flows/launcher.py`
- Modify: `src/config.py` (constantes du lanceur)
- Modify: `src/flows/decp_processing.py:240-242` (brancher `notify_exception_by_email` sur le lanceur)
- Modify: `template.env` (nouvelles variables, commentées)

**Interfaces:**

- Consumes: `ScalewayProvider` (Task 3), `execute_remote_run`, `RemoteRunConfig`, `RunState` (Task 4), `render_user_data` (Task 2).
- Produces: `@flow run_on_scaleway(env: str, deployment: str, branch: str, dist_dir: str | None, instance_type: str = "MEMORY3-X8C-64G", start_timeout_minutes: int = 20, max_duration_hours: int = 4) -> None`
- Produces (config) : `SCALEWAY_SSH_KEY_PATH` (clé privée du serveur actuel, défaut `~/.ssh/id_ed25519`), `SCALEWAY_DATA_VOLUME_NAMES = {"prod": "decp-data-prod", "dev": "decp-data-dev"}`, `INSTANCE_ENV_VARS = ["PREFECT_API_URL", "PREFECT_API_AUTH_STRING", "DATAGOUVFR_API_KEY", "S3_ENDPOINT_URL", "S3_BUCKET", "S3_ACCESS_KEY_ID", "S3_SECRET_ACCESS_KEY", "S3_REGION"]`.

- [ ] **Step 1: Implémenter le flow**

Câblage : `with concurrency(f"volume-{env}", raise_on_lease_renewal_failure=False)` ; `ScalewayProvider()` ; `user_data = render_user_data(...)` avec `INSTANCE_ENV_VARS` lus dans `os.environ` + `DECP_DATA_DIR=/data`, `PREFECT_HOME=/data/.prefect`, `DECP_DIST_DIR=/work/dist` ; `trigger = run_deployment(deployment, timeout=0, as_subflow=True).id` ; `get_state` via `get_client().read_flow_run(id).state` (SCHEDULED/PENDING → PENDING, RUNNING → RUNNING, COMPLETED → COMPLETED, tout état final autre → FAILED) ; `cancel` via `set_flow_run_state(id, Cancelling())` ; `sync_dist` via `rsync -a --delete -e "ssh -i <clé> -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null" root@<ip>:/work/dist/ <wip>/` (subprocess, 2 retries) ; `fetch_boot_log` via `ssh … tail -n 100 /var/log/cloud-init-output.log` (erreurs ignorées) ; `launcher_run_id` = id du run courant.

La création d'instance passée à `execute_remote_run` est enveloppée d'un retry `tenacity` (2 nouvelles tentatives, 60 s d'écart) : `provider=` reçoit un adaptateur dont `create_instance` est décoré, les autres méthodes déléguant telles quelles.

- [ ] **Step 2: Brancher l'email d'échec**

Ajouter `@run_on_scaleway.on_failure` au-dessus de `notify_exception_by_email` (import du flow dans `decp_processing.py`, ou déplacement du hook dans `src/flows/notify.py` importé par les deux flows : choisir l'option sans import circulaire).

- [ ] **Step 3: Vérifier l'import et la suite**

Run: `uv run python -c "from src.flows.launcher import run_on_scaleway; print(run_on_scaleway.name)" && uv run pytest -q`
Expected: `run-on-scaleway`, tous les tests passent.

- [ ] **Step 4: Commit**

```bash
git add src/flows/launcher.py src/config.py src/flows/decp_processing.py template.env
git commit -m "Flow lanceur run_on_scaleway (#189)"
```

---

### Task 6: Déploiements et script d'installation Scaleway

**Files:**

- Modify: `src/deployments.py:8-87` (decp-processing et sirene-preprocess, prod et dev ; scraps inchangés)
- Create: `script/scaleway_setup.py`

**Interfaces:**

- Consumes: `run_on_scaleway` (Task 5), `ScalewayProvider` (Task 3).
- Produces: les déploiements du tableau ci-dessous ; ressources Scaleway et Prefect permanentes.

| Déploiement                   | Pool            | Planning    | Entrypoint / paramètres                                                                                                                                           |
| ----------------------------- | --------------- | ----------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `decp-processing`             | `local`         | `0 5 * * *` | `src/flows/launcher.py:run_on_scaleway`, `env=prod`, `deployment=decp-processing/decp-processing-exec-prod`, `branch=main`, `dist_dir=/srv/shared/decp/prod/dist` |
| `decp-processing-exec-prod`   | `scaleway-prod` | —           | `decp_processing`, branche `main`, `job_variables.env` : `DECP_PROCESSING_PUBLISH=True`, `DECP_USE_CACHE=True`                                                    |
| `decp-processing-dev`         | `local`         | —           | lanceur, `env=dev`, `…-exec-dev`, `branch=dev`, `dist_dir=/srv/shared/decp/dev/dist`                                                                              |
| `decp-processing-exec-dev`    | `scaleway-dev`  | —           | `decp_processing`, branche `dev`, `DECP_PROCESSING_PUBLISH=False`, `DECP_USE_CACHE=False`                                                                         |
| `sirene-preprocess`           | `local`         | `0 1 3 * *` | lanceur, `env=prod`, `sirene-preprocess/sirene-preprocess-exec-prod`, `branch=main`, `dist_dir=None`                                                              |
| `sirene-preprocess-exec-prod` | `scaleway-prod` | —           | `sirene_preprocess`, branche `main`, `PREFECT_TASKS_REFRESH_CACHE=False`                                                                                          |
| `sirene-preprocess-dev`       | `local`         | —           | lanceur, `env=dev`, `…-exec-dev`, `branch=dev`, `dist_dir=None`                                                                                                   |
| `sirene-preprocess-exec-dev`  | `scaleway-dev`  | —           | `sirene_preprocess`, branche `dev`                                                                                                                                |

Les lanceurs utilisent la branche `main` du dépôt (code du lanceur) quel que soit `env`. `DECP_DIST_DIR` et `DECP_DATA_DIR` disparaissent des `job_variables` des déploiements `*-exec-*` (fournis par `/etc/decp.env`).

- [ ] **Step 1: Réécrire les déploiements concernés de `src/deployments.py`** selon le tableau.

- [ ] **Step 2: Écrire `script/scaleway_setup.py`** (idempotent) :

  - vérifier via `get_server_types_availability(zone)` que `MEMORY3-X8C-64G` est disponible dans `SCW_DEFAULT_ZONE`, sinon sortir en erreur avec la liste des zones à essayer ;
  - créer `decp-data-prod` et `decp-data-dev` (Block API, `perf_iops=5000`, 30 Go) s'ils n'existent pas ;
  - créer les pools `scaleway-prod` et `scaleway-dev` (type `process`) et les limites globales `volume-prod`, `volume-dev` (valeur 1) s'ils n'existent pas (client Prefect).
  - afficher l'état final (ids des volumes, pools, limites).

- [ ] **Step 3: Vérifier la syntaxe et l'idempotence à sec**

Run: `uv run python -c "import ast; ast.parse(open('src/deployments.py').read()); ast.parse(open('script/scaleway_setup.py').read())" && uv run pytest -q`
Expected: aucune erreur, tous les tests passent.

- [ ] **Step 4: Commit**

```bash
git add src/deployments.py script/scaleway_setup.py
git commit -m "Déploiements lanceur + exécution Scaleway, script d'installation (#189)"
```

---

### Task 7: Bascule et vérification réelle

Tâche d'exploitation, sans code. Chaque étape a un résultat vérifiable ; s'arrêter au premier écart.

> **Fusionner la branche dans `dev` en plus de `main` avant l'étape 7** : sinon l'instance dev exécute l'ancien code (prefect 3.7.0).

- [ ] **Step 1: Sauvegarder la base Prefect** — `pg_dump prefect > /root/prefect-$(date +%F).sql` sur le serveur ; fichier non vide.
- [ ] **Step 2: Venv du serveur depuis `uv.lock`** — dans le checkout `main` du serveur : `uv sync --frozen`, puis pointer `prefect-server` et `prefect-worker.service` sur ce venv ; `prefect version` affiche 3.9.1, `python -c "import polars; print(polars.__version__)"` affiche la version de `uv.lock`.
- [ ] **Step 3: Redémarrer serveur puis worker** ; l'UI répond, le worker `local` est « online » ; lancer un scrap et constater `Completed`.
- [ ] **Step 4: `.env` du serveur** — ajouter `SCW_*`, `SCALEWAY_SSH_KEY_PATH` ; générer la paire de clés si absente.
- [ ] **Step 5: `uv run python script/scaleway_setup.py`** deux fois ; la seconde ne crée rien.
- [ ] **Step 6: `uv run python src/deployments.py`** ; les huit déploiements du tableau de Task 6 apparaissent dans l'UI.
- [ ] **Step 7: Vérifications préalables au run dev** — vérifier `prefect.maudry.com/api` joignable depuis l'extérieur avec `PREFECT_API_AUTH_STRING` (curl depuis une autre machine), et que l'API Scaleway accepte les tags `env:prod`/`flow-run:<uuid>` lors d'une création de test.
- [ ] **Step 8: Run dev de bout en bout** — déclencher `decp-processing-dev` : instance créée, worker `scaleway-dev` online, run `Completed`, `/srv/shared/decp/dev/dist` mis à jour, instance absente de `scw instance server list` et aucun volume `*-boot` restant ; noter pic mémoire (`free -g` via ssh pendant le run), durée et coût.
- [ ] **Step 9: Pré-remplir le volume prod** — lancer une instance via `sirene-preprocess` (prod) ou une instance manuelle avec le volume prod monté, et y copier depuis le serveur actuel `resource_cache/`, `sirene_*/`, `.prefect/storage/` (~5 Go) par rsync ; vérifier les tailles.
- [ ] **Step 10: Run prod manuel** — déclencher `decp-processing` : `Completed`, publication data.gouv.fr et S3, `/srv/shared/decp/prod/dist` à jour, decp.info sert les nouvelles données, instance détruite.
- [ ] **Step 11: Documentation** — `CHANGELOG.md` (exécution sur instance éphémère, Prefect 3.9.1) ; commit.

```bash
git add CHANGELOG.md
git commit -m "Changelog : exécution sur instance Scaleway éphémère (#189)"
```

**Retour arrière** : redéployer la version précédente de `src/deployments.py` (tout sur `local`) ; restaurer la base depuis la sauvegarde de Step 1 si la montée de version de Prefect pose problème.
