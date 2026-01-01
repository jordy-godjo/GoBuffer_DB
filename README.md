# GoBuffer_DB

# Guide d'utilisation — GoBuffer_DB(cross-plateforme)

Ce document contient des instructions pas-à-pas pour exécuter et vérifier le mini-SGBD sans la présence de l'auteur. Il prend en charge Windows (PowerShell), Linux/macOS (bash) et autres OS. Il fournit un scénario d'exemple (`commands.txt`), un fichier CSV d'exemple, des patterns attendus, et des scripts pour exécuter automatiquement le scénario.

> Positionnez-vous dans le répertoire du projet (exemples ci‑dessous). Utilisez des chemins relatifs — le projet ne doit pas dépendre d'un chemin absolu ni d'un OS spécifique.

PowerShell (Windows) :

```powershell
Set-Location -LiteralPath 'C:\path\to\malzahar-project\Projet_BDDA'
```

Bash (Linux / macOS) :

```bash
cd /path/to/malzahar-project/Projet_BDDA
```

## 1 — Pré-requis et build

- Go toolchain installé si vous voulez recompiler (`go build`).

Si l'exécutable n'est pas fourni :

PowerShell :

```powershell
go build -o minisgbd.exe ./src
```

Bash :

```bash
go build -o minisgbd ./src
```

Le binaire accepte le flag `-config config.txt` (fichier `config.txt` à la racine du projet).

## 2 — Préparer un état propre (recommandé)

Pour une démonstration déterministe, sauvegardez puis supprimez tout dossier `data/` existant.

PowerShell :

```powershell
Copy-Item -Path .\data -Destination .\data.bak -Recurse -ErrorAction SilentlyContinue
Remove-Item -Path .\data -Recurse -Force -ErrorAction SilentlyContinue
```

Bash :

```bash
cp -r data data.bak 2>/dev/null || true
rm -rf data || true
```

Cela évite les problèmes causés par des données persistantes créées par des versions antérieures.

## 3 — Fichiers fournis pour la démonstration

Créez (ou vérifiez la présence) des fichiers suivants à la racine du projet :

- `minisgbd` ou `minisgbd.exe` : le binaire (ou reconstruisez-le avec `go build`).
- `config.txt` : fourni dans le repo.
- `commands.txt` : séquence de commandes à exécuter (exemple ci‑dessous).
- `Fruit_extra.csv` : CSV simple pour `APPEND` (exemple ci‑dessous).
- `expected.txt` : patterns/textes attendus pour vérification rapide (exemple ci‑dessous).

### Exemple `commands.txt` (scénario README)

Copiez ce contenu dans `commands.txt` :

```
CREATE TABLE Fruit (id:INT,name:VARCHAR(20),price:REAL)
INSERT INTO Fruit VALUES (1,'Pomme',1.2)
INSERT INTO Fruit VALUES (2,'Poire',0.8)
INSERT INTO Fruit VALUES (3,'Banane',0.5)
SELECT * FROM Fruit f
DELETE Fruit f WHERE f.id = 2
UPDATE Fruit f SET f.price = 0.6 WHERE f.name = 'Banane'
APPEND INTO Fruit ALLRECORDS(Fruit_extra.csv)
SELECT f.id,f.name,f.price FROM Fruit f
EXIT
```

### Exemple `Fruit_extra.csv`

Créez `Fruit_extra.csv` contenant exactement :

```
4,Orange,0.9
5,Kaki,1.1
```

Ce CSV doit être simple (pas de virgules internes ni de guillemets complexes). Le parseur CSV actuel est basique; si vos CSV sont complexes, demandez-moi d'ajouter `encoding/csv`.

### Exemple `expected.txt` (patterns attendus)

Vous pouvez utiliser un `expected.txt` contenant des patterns simples que la sortie doit contenir, par exemple :

```
1,Pomme,1.2
3,Banane,0.6
4,Orange,0.9
5,Kaki,1.1
```

La vérification doit être tolérante (recherche de sous-chaînes) car le format exact d'affichage peut varier légèrement.

## 4 — Lancer le scénario et capturer la sortie

PowerShell :

```powershell
Get-Content .\commands.txt -Raw | .\minisgbd.exe -config config.txt > .\output.txt

En cmd, vous pouvez utiliser : .\minisgbd.exe -config config.txt < .\commands.txt > .\output.txt
```

Bash :

```bash
./minisgbd -config config.txt < commands.txt > output.txt
```

Ensuite, vérifier `output.txt` manuellement ou automatiquement.

### Usage `-fresh` (réinitialiser l'état persistant)

Si vous souhaitez démarrer sur un état vierge (très utile pour une démo déterministe), utilisez le flag `-fresh`. Le programme fera une sauvegarde automatique du dossier `data/` (nommée `data.bak-YYYYmmdd-HHMMSS`) puis supprimera `data/` avant de démarrer.

Exemples :

PowerShell (script non interactif) :

```powershell
Get-Content .\commands.txt -Raw | .\minisgbd.exe -config config.txt -fresh --yes > .\output.txt
```

Bash :

```bash
./minisgbd -config config.txt -fresh --yes < commands.txt > output.txt
```

Note : sans `--yes`, le programme demandera confirmation interactive avant de supprimer les données ; `--yes` force le comportement non interactif pour les scripts.

### Vérification automatique (PowerShell)

```powershell
$expected = Get-Content .\expected.txt
$out = Get-Content .\output.txt -Raw
$allok = $true
foreach ($pat in $expected) {
		if (-not ($out -match [regex]::Escape($pat))) { $allok = $false; Write-Host "Miss: $pat" }
}
if ($allok) { Write-Host "PASS" } else { Write-Host "FAIL"; exit 2 }
```

### Vérification automatique (Bash)

```bash
allok=true
for pat in $(cat expected.txt); do
	if ! grep -Fq "$pat" output.txt; then
		echo "Miss: $pat"
		allok=false
	fi
done
if $allok; then echo PASS; exit 0; else echo FAIL; exit 2; fi
```

## 5 — Checklist (quoi vérifier)

- [ ] `CREATE TABLE` : la table est créée sans erreur.
- [ ] `INSERT` : les tuples insérés apparaissent dans `SELECT`.
- [ ] `APPEND` : le CSV simple est importé (les lignes du CSV apparaissent).
- [ ] `DELETE` : les tuples supprimés ne sont plus visibles.
- [ ] `UPDATE` : les valeurs modifiées sont visibles (note : UPDATE = suppression + insertion interne).
- [ ] Persistance : arrêter le binaire, relancer avec le même `config.txt` et vérifier que les tables/données persistent.
- [ ] Obligatoire : lancer `go test ./...` pour valider les tests unitaires.

## 6 — Erreurs courantes et dépannage rapide

- CSV complexe -> colonnes décalées : utiliser un CSV simple ou insérer manuellement via `INSERT`.
- Type mismatch (ex: string pour INT) -> vérifier le type dans `CREATE TABLE` et la valeur insérée.
- Si le programme boucle ou plante : supprimer `data/` (voir section 2) et relancer.
- Si vous voyez "unknown column type: REAL" : utilisez le binaire fourni par ce dépôt (ou reconstruisez après pull) — la version actuelle accepte `REAL`.

C'est tout, merci !
