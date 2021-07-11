import glob
import subprocess
import tempfile
import time

from jinja2 import Environment, FileSystemLoader, select_autoescape
from os import path

import fire

import yaml
from mergedeep import mergedeep


class Deployer:
    def __init__(self, env: str):
        self.env = env
        self.tmpl_env = Environment(
            loader=FileSystemLoader(Deployer._local_path('')),
            autoescape=select_autoescape()
        )

        config_file = open(Deployer._local_path(f'environments/{self.env}.yaml'), 'r')
        self.config = yaml.safe_load(config_file)

    def helm_install(self, app: str, chart: str, overlay_values: str = None, extra_args: list = None):
        cmd = ['helm', 'install', app, chart]
        if overlay_values is not None:
            if overlay_values.endswith('.jinja2'):
                # a bit ugly: reverse out to get back the relative path
                # for Jinja2 template resolution
                rel_path = path.relpath(overlay_values, path.dirname(__file__))
                yaml_tmpl = self.tmpl_env.get_template(rel_path)
                generated_yaml_txt = yaml_tmpl.render(self.config)
                with tempfile.NamedTemporaryFile() as tmp:
                    tmp.write(generated_yaml_txt.encode('utf-8'))
                    tmp.flush()
                    if extra_args is not None:
                        cmd.extend(extra_args)
                    cmd.extend(['-f', tmp.name])
                    Deployer._run_command(cmd)
            else:
                cmd.extend(['-f', Deployer._local_path(overlay_values)])
                if extra_args is not None:
                    cmd.extend(extra_args)
                Deployer._run_command(cmd)

    def kube_install(self, yaml_path: str, namespace: str = None):
        if self.env == 'dev':
            kubectl = 'kubectl'
        else:
            kubectl = 'microk8s.kubectl'

        cmd = [kubectl, 'apply', '-f', yaml_path]
        if namespace is not None:
            cmd.extend(['-n', namespace])
        Deployer._run_command(cmd)

    def deploy_all(self, base_dir: str):
        scan_dir = Deployer._local_path(f'{base_dir}')
        input_paths = []
        for ext in ('*.yaml', '*.yaml.jinja2'):
            input_paths.extend(glob.glob(f'{scan_dir}/{ext}'))
        for input_path in input_paths:
            self.deploy(input_path)

    def deploy(self, input_path: str, extra_vars: dict = None):
        print(f'processing {input_path}')
        if input_path.endswith('.jinja2'):
            # a bit ugly: reverse out to get back the relative path
            # for Jinja2 template resolution
            rel_path = path.relpath(input_path, path.dirname(__file__))
            yaml_tmpl = self.tmpl_env.get_template(rel_path)

            tmpl_vars = self.config
            if extra_vars is not None:
                mergedeep.merge(tmpl_vars, extra_vars)
            generated_yaml_txt = yaml_tmpl.render(tmpl_vars)
            with tempfile.NamedTemporaryFile() as tmp:
                tmp.write(generated_yaml_txt.encode('utf-8'))
                tmp.flush()
                self.kube_install(tmp.name)
        else:
            self.kube_install(input_path)

    @staticmethod
    def _local_path(rel_path: str):
        return path.join(path.dirname(__file__), f'{rel_path}')

    @staticmethod
    def _run_command(args: list):
        subprocess.run(args)


# noinspection PyDefaultArgument
def install_serenity(env: str = 'dev', components: list = ['core', 'db', 'infra', 'strategies', 'research']):
    if isinstance(components, str):
        components = [components]

    # noinspection PyUnresolvedReferences
    component_set = set([x.lower() for x in components])
    deployer = Deployer(env)

    # install Helm charts for infrastructure
    if 'infra' in component_set:
        deployer.helm_install('consul', 'hashicorp/consul', f'consul/consul-helm-values.yaml.jinja2',
                              ['--set', 'global.name=consul'])
        print('waiting for Consul to come up')
        time.sleep(5)
        get_svc = subprocess.Popen(['kubectl', 'get', 'svc', 'consul-dns', '-o', 'jsonpath="{.spec.clusterIP}"'],
                                   stdout=subprocess.PIPE)
        extra_vars = {'consul_dns_ip': get_svc.stdout.read().decode('utf8').replace('"', '')}
        print(f'got Consul DNS IP: {extra_vars["consul_dns_ip"]}')
        deployer.deploy('consul/consul-dns-configmap.yaml.jinja2', extra_vars)
        deployer.helm_install('prometheus', 'prometheus-community/prometheus',
                              f'telemetry/prometheus-helm-values.yaml')
        deployer.helm_install('alertmanager', 'prometheus-community/alertmanager',
                              f'telemetry/alertmanager-helm-values.yaml')
        deployer.helm_install('grafana', 'grafana/grafana', f'telemetry/grafana-helm-values.yaml')

    # install requested Kubernetes objects
    if 'core' in component_set:
        deployer.deploy_all('feedhandlers')

    if 'db' in component_set:
        deployer.deploy_all('db')

    if 'strategies' in component_set:
        deployer.deploy_all('strategies')

    if 'research' in component_set:
        deployer.deploy_all('research')


if __name__ == '__main__':
    fire.Fire(install_serenity)
