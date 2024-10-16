import requests
from config import ACCESS_TOKEN
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed


ORG_NAME = 'Netflix'


def get_open_repositories(org):
    url = f'https://api.github.com/orgs/{org}/repos'
    headers = {
        'Authorization': f'token {ACCESS_TOKEN}',
        'Accept': 'application/vnd.github.v3+json'
    }

    repositories = []
    page = 1
    while True:
        response = requests.get(url, headers=headers, params={'page': page, 'per_page': 100})

        if response.status_code != 200:
            print(f"Ошибка: {response.status_code} - {response.json().get('message')}")
            break

        repos = response.json()

        if not repos:
            break

        repositories.extend(repos)
        page += 1

    return repositories


def get_commits(repo_full_name):
    url = f'https://api.github.com/repos/{repo_full_name}/commits'
    headers = {
        'Authorization': f'token {ACCESS_TOKEN}',
        'Accept': 'application/vnd.github.v3+json'
    }

    commits_count = defaultdict(int)
    page = 1

    while True:
        response = requests.get(url, headers=headers, params={'page': page, 'per_page': 100})
        if response.status_code != 200:
            print(f"Ошибка при получении коммитов: {response.status_code} - {response.json().get('message')}")
            break

        commits = response.json()

        if not commits:
            break

        for commit in commits:
            commit_message = commit['commit']['message']
            if not commit_message.startswith('Merge pull request'):
                author_email = commit['commit']['author']['email']
                commits_count[author_email] += 1

        page += 1

    return commits_count


def check_rate_limit():
    url = 'https://api.github.com/rate_limit'
    headers = {
        'Authorization': f'token {ACCESS_TOKEN}',
        'Accept': 'application/vnd.github.v3+json'
    }
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        rate_limit_info = response.json()
        core_limit = rate_limit_info['resources']['core']
        search_limit = rate_limit_info['resources']['search']

        print(f"Лимит запросов (core): {core_limit['limit']}")
        print(f"Оставшиеся запросы (core): {core_limit['remaining']}")
        print(f"Сброс лимита (core): {core_limit['reset']} (время в Unix timestamp)")

        print(f"Лимит запросов (search): {search_limit['limit']}")
        print(f"Оставшиеся запросы (search): {search_limit['remaining']}")
        print(f"Сброс лимита (search): {search_limit['reset']} (время в Unix timestamp)")
    else:
        print(f"Ошибка: {response.status_code} - {response.json().get('message')}")


def main():
    repositories = get_open_repositories(ORG_NAME)

    total_commits_by_author = defaultdict(int)

    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_repo = {executor.submit(get_commits, repo['full_name']): repo for repo in repositories}

        for future in as_completed(future_to_repo):
            repo = future_to_repo[future]
            print(f"Обработка репозитория: {repo['full_name']}")
            try:
                commits_count = future.result()
                for email, count in commits_count.items():
                    total_commits_by_author[email] += count
                print(f"Обработка репозитория завершена: {repo['full_name']}")
            except Exception as exc:
                print(f"Ошибка при обработке репозитория {repo['full_name']}: {exc}")

    top_authors = sorted(total_commits_by_author.items(), key=lambda x: x[1], reverse=True)[:100]

    print("Топ-100 самых активных авторов:")
    for email, count in top_authors:
        print(f"{email}: {count} коммитов")


if __name__ == '__main__':
    main()
    check_rate_limit()
