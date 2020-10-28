#!/usr/bin/env python3

from aws_cdk import core

from cdk_projects.cdk_projects_stack import CdkProjectsStack


app = core.App()
CdkProjectsStack(app, "cdk-projects")

app.synth()
