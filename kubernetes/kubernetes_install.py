from os import path


def open_kubernetes_yaml(rel_path: str):
    return open(path.join(path.dirname(__file__), f'{rel_path}'))


def deploy(rel_path: str):
    print(open_kubernetes_yaml(rel_path))


if __name__ == '__main__':
    # helm install consul hashicorp/consul --set global.name=consul -f kubernetes/consul/dev-config.yaml

    deploy('research/01_serenity-jupyterlab-storage.yaml')
