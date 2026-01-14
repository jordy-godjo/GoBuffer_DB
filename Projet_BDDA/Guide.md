## GoBuffer_DB — Guide d'exécution

Ce guide décrit le projet GoBuffer_DB, explique comment le construire, l'exécuter et lancer les tests. Il se concentre sur les instructions pratiques et la documentation technique nécessaire pour utiliser et valider le mini-SGBD.

## Résumé

GoBuffer_DB est un mini-SGBD implémenté en Go. Il fournit :
- un buffer pool simple,
- une couche d'accès disque bas niveau,
- une logique de relations et d'enregistrements,
- des commandes minimales pour créer, insérer et lire des enregistrements.

## Contrat (input / output)
- Entrée : commandes depuis le prompt interactif ou via fichiers de configuration (`config.txt`).
- Sortie : logs et messages sur stdout, fichiers de données binaires dans `relation/testdb/BinData/`.

## Prérequis
- Go 1.16+ (recommandé : Go 1.20)
- ~200 MB d'espace disque libre
- PowerShell (Windows) ou un shell POSIX pour Linux/macOS

## Préparation

PowerShell (Windows) :

```powershell
Set-Location -LiteralPath 'C:\path\to\malzahar-project\Projet_BDDA'
```

Bash (Linux / macOS) :

```bash
cd /path/to/malzahar-project/Projet_BDDA
```
## Compilation

Le point d'entrée est dans `src/`. Pour compiler (PowerShell) :

```powershell
go build -o minisgbd.exe ./src
```

Sur Linux/macOS :

```bash
go build -o minisgbd ./src
```

Le binaire `minisgbd(.exe)` est créé à la racine `Projet_BDDA`.

## Exécution (mode interactif)

Par défaut le programme lit `config.txt`. Pour lancer :

```powershell
.\minisgbd.exe -config config.txt
```

Option utile :
- `-fresh` : démarre avec un état propre (supprime / réinitialise les fichiers persistants selon l'implémentation).

```powershell
.\minisgbd.exe -config config.txt -fresh
```

Le programme ouvre un prompt interactif acceptant des commandes SQL simplifiées.

## Scripts fournis

Utiliser les wrappers fournis si souhaité :
- `run.ps1` / `run.bat` — lancement de l'exécutable sous Windows
- `run-tests.ps1` — exécution des tests unitaires

## Tests unitaires

Exécuter les tests :

```powershell
go test ./...
```

Pour plus de verbosité :

```powershell
go test ./... -v
```

Les tests couvrent plusieurs packages (`buffer`, `disk`, `relation`, `db`, `sgbd`).

## Structure du projet

```
Projet_BDDA/
├─ src/                   # code d'entrée (main)
├─ buffer/                # gestion du buffer
├─ disk/                  # accès disque bas niveau
├─ relation/              # logique relationnelle (record, relation.go)
├─ db/                    # manager de la base
├─ sgbd/                  # scénarios et orchestration
├─ config/                # configuration (db_config.go)
├─ run.ps1, run.bat       # scripts d'exécution
├─ config.txt             # configuration par défaut
├─ go.mod                 # modules go
└─ README.md / Guide.md   # documentation
```

## Architecture (points clés)
- Buffer pool simple pour réduire les accès disque.
- Fichiers binaires pour la persistance (voir `relation/testdb/BinData/`).
- Conception modulaire facilitant les tests unitaires et l'observation du comportement.

## Résolution de problèmes courants
- Erreur « cannot find module » : exécutez `go mod tidy` à la racine.
- Permissions sur fichiers : vérifiez les droits d'écriture (Windows : exécuter PowerShell en admin si nécessaire).
- Tests qui échouent : lancer `go test ./<pkg>` pour isoler le package en erreur.

## Contribution

- Forkez, créez une branche `feature/...` et ouvrez une PR.
- Respectez et ajoutez des tests pour toute nouvelle fonctionnalité.

## Licence

Le projet contient un fichier `LICENSE` à la racine. Respectez la licence associée.

## Annexes — commandes utiles (PowerShell)

```powershell
# se placer dans le projet
cd C:\Users\NEC\Desktop\projet\malzahar-project\Projet_BDDA

# construire
go build -o minisgbd.exe ./src

# exécuter
.\minisgbd.exe -config config.txt

# exécuter et réinitialiser
.\minisgbd.exe -config config.txt -fresh

# tests
go test ./... -v

# tidy modules
go mod tidy
```




