#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Static detection of runtime-varying values in Dag and task constructor arguments.

A ``datetime.now()``, ``uuid4()``, or similar runtime-varying value passed into a Dag or
task constructor changes the serialized Dag on every parse, so Airflow records a new Dag
version each time -- unbounded version inflation. This module is adapted from Apache
Airflow's Dag version inflation checker (see ``PROVENANCE.md`` for the pinned upstream
commit and the full deviation list) and backs the ``test_no_runtime_varying_dag_args``
smoke item. Everything here is pure ``ast`` analysis: no Airflow import, no Dag execution.

The detection is deliberately conservative, mirroring upstream: values are tracked through
top-level imports and simple assignments only, and a call outside a ``with DAG`` block
counts as a task only when it names a registered Dag instance or its callee name ends in
``Operator``/``task``/``Sensor``.

References:
    https://github.com/apache/airflow/pull/59430
    https://github.com/apache/airflow/blob/e7efeedccb6b8731b829707d5ea16f7bf0861b5b/airflow-core/src/airflow/utils/dag_version_inflation_checker.py
    https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#top-level-python-code
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from enum import Enum

# (module, callable) pairs whose calls produce a different value on every evaluation.
RUNTIME_VARYING_CALLS: frozenset[tuple[str, str]] = frozenset(
    {
        ("datetime", "now"),
        ("datetime", "today"),
        ("datetime", "utcnow"),
        ("date", "today"),
        ("time", "time"),
        ("time", "localtime"),
        ("random", "random"),
        ("random", "randint"),
        ("random", "choice"),
        ("random", "uniform"),
        ("uuid", "uuid4"),
        ("uuid", "uuid1"),
        ("pendulum", "now"),
        ("pendulum", "today"),
        ("pendulum", "yesterday"),
        ("pendulum", "tomorrow"),
    }
)


class VaryingContext(str, Enum):
    """Constructor context a varying value was passed into, phrased for failure messages."""

    DAG_CONSTRUCTOR = "the Dag constructor"
    TASK_CONSTRUCTOR = "a task constructor"
    TASK_IN_DAG_BLOCK = "a task inside a with-Dag block"
    TASK_DECORATOR = "a task decorator"


@dataclass(frozen=True)
class RuntimeVaryingFinding:
    """One runtime-varying value found in a Dag or task constructor argument.

    Parameters:
        line: int containing the 1-indexed first source line of the constructor call.
        end_line: int containing the 1-indexed last source line of the constructor call.
        snippet: str containing the varying sub-expression's source text.
        context: str containing the constructor context phrase for the failure message.
    """

    line: int
    end_line: int
    snippet: str
    context: str


def _snippet(node: ast.expr, source: str) -> str:
    """Render one expression's source text for a failure message.

    Parameters:
        node: ast.expr to render.
        source: str containing the full module source.

    Returns:
        str containing the expression's original source segment, or its unparsed form when
        the node lacks the position attributes a segment lookup needs.
    """

    return ast.get_source_segment(source, node) or ast.unparse(node)


class RuntimeVaryingValueAnalyzer:
    """Track and detect expressions whose value changes on every evaluation.

    The three binding maps are shared with the owning visitor and mutated there as the
    module body is walked; the analyzer only reads them.
    """

    def __init__(
        self,
        varying_vars: dict[str, tuple[int, str]],
        imports: dict[str, str],
        from_imports: dict[str, tuple[str, str]],
        source: str,
    ) -> None:
        """Bind the shared binding maps and the module source.

        Parameters:
            varying_vars: dict[str, tuple[int, str]] mapping tainted variable names to
                their taint line and originating varying-expression text.
            imports: dict[str, str] mapping ``import``-bound names to module names.
            from_imports: dict[str, tuple[str, str]] mapping ``from ... import``-bound
                names to their (module, original name) pair.
            source: str containing the full module source, for snippet rendering.
        """

        self.varying_vars = varying_vars
        self.imports = imports
        self.from_imports = from_imports
        self.source = source

    def get_varying_source(self, node: ast.expr) -> str | None:
        """Return the varying expression's source text when one is reachable in a node.

        Walks calls, method chains, tainted variable references, f-strings, binary
        operations, collection literals, list comprehensions, and dictionaries.

        Parameters:
            node: ast.expr to inspect.

        Returns:
            str | None containing the varying expression's source text, or ``None`` when
            the node is statically stable.
        """

        if isinstance(node, ast.Call):
            if self.is_runtime_varying_call(node.func):
                return _snippet(node, self.source)
            if self.get_varying_argument(node) is not None:
                return _snippet(node, self.source)
            if isinstance(node.func, ast.Attribute):
                return self.get_varying_source(node.func.value)
        if isinstance(node, ast.Name) and node.id in self.varying_vars:
            _, varying = self.varying_vars[node.id]
            return varying
        if isinstance(node, ast.JoinedStr):
            return self.get_varying_fstring(node)
        if isinstance(node, ast.BinOp):
            return self.get_varying_source(node.left) or self.get_varying_source(node.right)
        if isinstance(node, ast.List | ast.Tuple | ast.Set):
            return self.get_varying_collection(node.elts)
        if isinstance(node, ast.ListComp):
            return self.get_varying_source(node.elt)
        if isinstance(node, ast.Dict):
            return self.get_varying_dict(node)
        return None

    def get_varying_argument(self, node: ast.Call) -> str | None:
        """Return the first varying argument's source text in one call.

        Parameters:
            node: ast.Call whose positional and keyword arguments to inspect.

        Returns:
            str | None containing the first varying argument's source text, or ``None``
            when every argument is statically stable.
        """

        for arg in node.args:
            if varying := self.get_varying_source(arg):
                return varying
        for keyword in node.keywords:
            if varying := self.get_varying_source(keyword.value):
                return varying
        return None

    def get_varying_fstring(self, node: ast.JoinedStr) -> str | None:
        """Return the first varying formatted value's source text in one f-string.

        Parameters:
            node: ast.JoinedStr to inspect.

        Returns:
            str | None containing the varying expression's source text, or ``None``.
        """

        for value in node.values:
            if isinstance(value, ast.FormattedValue) and (
                varying := self.get_varying_source(value.value)
            ):
                return varying
        return None

    def get_varying_collection(self, elements: list[ast.expr]) -> str | None:
        """Return the first varying element's source text in one collection literal.

        Parameters:
            elements: list[ast.expr] containing the collection's elements.

        Returns:
            str | None containing the varying expression's source text, or ``None``.
        """

        for element in elements:
            if varying := self.get_varying_source(element):
                return varying
        return None

    def get_varying_dict(self, node: ast.Dict) -> str | None:
        """Return the first varying key or value's source text in one dict literal.

        A ``**expansion`` entry carries a ``None`` key and only its value is inspected.

        Parameters:
            node: ast.Dict to inspect.

        Returns:
            str | None containing the varying expression's source text, or ``None``.
        """

        for key, value in zip(node.keys, node.values, strict=False):
            if key is not None and (varying := self.get_varying_source(key)):
                return varying
            if varying := self.get_varying_source(value):
                return varying
        return None

    def is_runtime_varying_call(self, func: ast.expr) -> bool:
        """Report whether one callee resolves to a certified runtime-varying callable.

        Parameters:
            func: ast.expr containing the call's callee.

        Returns:
            bool indicating the callee is one of `RUNTIME_VARYING_CALLS`.
        """

        if isinstance(func, ast.Attribute):
            return self.is_runtime_varying_attribute_call(func)
        if isinstance(func, ast.Name):
            return self.is_runtime_varying_name_call(func)
        return False

    def is_runtime_varying_attribute_call(self, attr: ast.Attribute) -> bool:
        """Report whether an attribute callee like ``datetime.now`` is runtime-varying.

        Parameters:
            attr: ast.Attribute containing the callee.

        Returns:
            bool indicating the (module, method) pair is in `RUNTIME_VARYING_CALLS`.
        """

        method_name = attr.attr
        if isinstance(attr.value, ast.Name):
            module_or_alias = attr.value.id
            actual_module = self.imports.get(module_or_alias, module_or_alias)
            if module_or_alias in self.from_imports:
                _, actual_module = self.from_imports[module_or_alias]
            return (actual_module, method_name) in RUNTIME_VARYING_CALLS
        if isinstance(attr.value, ast.Attribute):
            inner = attr.value
            if isinstance(inner.value, ast.Name):
                return (inner.attr, method_name) in RUNTIME_VARYING_CALLS
        return False

    def is_runtime_varying_name_call(self, func: ast.Name) -> bool:
        """Report whether a bare-name callee bound by ``from ... import`` is varying.

        Any dotted part of the source module may match -- ``from vendor.random import
        random`` is flagged -- which upstream accepts as a cheap, conservative test.

        Parameters:
            func: ast.Name containing the callee.

        Returns:
            bool indicating the callee resolves to a `RUNTIME_VARYING_CALLS` entry.
        """

        if func.id not in self.from_imports:
            return False
        module, original_name = self.from_imports[func.id]
        return any((part, original_name) in RUNTIME_VARYING_CALLS for part in module.split("."))


class DagTaskDetector:
    """Identify Dag and task constructor calls from import bindings and context.

    Dag detection requires the callee to resolve through a ``from airflow...`` import;
    task detection accepts a call naming a registered Dag instance as an argument, or --
    inside a ``with DAG`` block -- a callee name ending in ``operator``/``task``/``sensor``.
    """

    def __init__(self, from_imports: dict[str, tuple[str, str]]) -> None:
        """Bind the shared from-import map and start with no known Dag instances.

        Parameters:
            from_imports: dict[str, tuple[str, str]] mapping ``from ... import``-bound
                names to their (module, original name) pair, shared with the visitor.
        """

        self.from_imports = from_imports
        self.dag_instances: set[str] = set()
        self.is_in_dag_context = False
        self.function_def_context: str | None = None

    def is_dag_constructor(self, node: ast.Call) -> bool:
        """Report whether one call constructs an Airflow Dag.

        Parameters:
            node: ast.Call to classify.

        Returns:
            bool indicating the callee resolves to Airflow's ``DAG`` class or ``dag``
            decorator through the file's from-imports.
        """

        # `from airflow import sdk` followed by `sdk.DAG(...)`.
        if (
            isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id in self.from_imports
        ):
            module, _ = self.from_imports[node.func.value.id]
            if _is_airflow_module(module) and node.func.attr in ("DAG", "dag"):
                return True
        # `from airflow import DAG` or `from airflow.decorators import dag`.
        if isinstance(node.func, ast.Name) and node.func.id in self.from_imports:
            module, original = self.from_imports[node.func.id]
            if _is_airflow_module(module) and original in ("DAG", "dag"):
                return True
        return False

    def is_task_constructor(self, node: ast.Call) -> bool:
        """Report whether one call constructs an Airflow task.

        Parameters:
            node: ast.Call to classify.

        Returns:
            bool indicating the call is a task-named call inside a ``with DAG`` block or
            names a registered Dag instance as an argument.
        """

        if self.is_in_dag_context and self.is_task_named(node.func):
            return True
        for arg in node.args:
            if isinstance(arg, ast.Name) and arg.id in self.dag_instances:
                return True
        for keyword in node.keywords:
            if isinstance(keyword.value, ast.Name) and keyword.value.id in self.dag_instances:
                return True
        return False

    def is_task_decorator(self, node: ast.expr) -> bool:
        """Report whether one decorator expression is a task decorator.

        Parameters:
            node: ast.expr containing the decorator.

        Returns:
            bool indicating the decorator's name (or called name) is task-shaped.
        """

        if isinstance(node, ast.Name | ast.Attribute):
            return self.is_task_named(node)
        if isinstance(node, ast.Call):
            return self.is_task_decorator(node.func)
        return False

    def is_task_named(self, node: ast.expr) -> bool:
        """Report whether one name or attribute chain carries a task-shaped name.

        Parameters:
            node: ast.expr containing the callee or decorator name.

        Returns:
            bool indicating some segment of the name ends in ``operator``, ``task``, or
            ``sensor`` (case-insensitive).
        """

        if isinstance(node, ast.Name):
            return _is_task_function_name(node.id)
        if isinstance(node, ast.Attribute):
            if _is_task_function_name(node.attr):
                return True
            return self.is_task_named(node.value)
        return False

    def register_dag_instance(self, var_name: str) -> None:
        """Register one variable name as holding a Dag instance.

        Parameters:
            var_name: str containing the bound variable name.
        """

        self.dag_instances.add(var_name)


def _is_airflow_module(module: str) -> bool:
    """Report whether one from-import module path lives under the ``airflow`` namespace.

    Parameters:
        module: str containing the dotted module path.

    Returns:
        bool indicating the path is ``airflow`` or a submodule of it.
    """

    return module == "airflow" or module.startswith("airflow.")


def _is_task_function_name(name: str) -> bool:
    """Report whether one bare name is task-shaped.

    Parameters:
        name: str containing the callee or decorator name segment.

    Returns:
        bool indicating the name ends in ``operator``, ``task``, or ``sensor``
        (case-insensitive).
    """

    lowered = name.lower()
    return lowered.endswith(("operator", "task", "sensor"))


class _RuntimeVaryingDagArgsVisitor(ast.NodeVisitor):
    """Walk one parsed Dag module and collect runtime-varying constructor findings.

    Assigned constructor calls are classified in ``visit_Assign`` and bare-statement or
    with-item calls in ``visit_Call`` -- the two paths together encode exactly which
    statement forms upstream scans, so they are deliberately not unified. Neither path
    recurses into its call's children, matching upstream: a constructor nested inside
    another expression is not classified.
    """

    def __init__(self, source: str) -> None:
        """Initialize empty binding maps and the analyzer/detector collaborators.

        Parameters:
            source: str containing the full module source, for snippet rendering.
        """

        self.findings: list[RuntimeVaryingFinding] = []
        self.imports: dict[str, str] = {}
        self.from_imports: dict[str, tuple[str, str]] = {}
        self.varying_vars: dict[str, tuple[int, str]] = {}
        self.varying_functions: dict[str, RuntimeVaryingFinding] = {}
        self.value_analyzer = RuntimeVaryingValueAnalyzer(
            self.varying_vars, self.imports, self.from_imports, source
        )
        self.dag_detector = DagTaskDetector(self.from_imports)

    def visit_Import(self, node: ast.Import) -> None:
        """Record the module names one ``import`` statement binds.

        Parameters:
            node: ast.Import to record.
        """

        for alias in node.names:
            self.imports[alias.asname or alias.name] = alias.name

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        """Record the (module, original name) pairs one ``from ... import`` binds.

        Parameters:
            node: ast.ImportFrom to record.
        """

        if node.module:
            for alias in node.names:
                self.from_imports[alias.asname or alias.name] = (node.module, alias.name)

    def visit_Assign(self, node: ast.Assign) -> None:
        """Classify an assignment as a Dag binding, a task binding, or a taint source.

        Parameters:
            node: ast.Assign to classify.
        """

        value = node.value
        if isinstance(value, ast.Call) and self.dag_detector.is_dag_constructor(value):
            self._register_dag_instances(node.targets)
            self._check_and_record(value, VaryingContext.DAG_CONSTRUCTOR)
        elif isinstance(value, ast.Call) and self.dag_detector.is_task_constructor(value):
            self._check_and_record(value, VaryingContext.TASK_CONSTRUCTOR)
        else:
            self._track_varying_assignment(node)

    def visit_Call(self, node: ast.Call) -> None:
        """Classify a bare call and surface deferred factory findings.

        Parameters:
            node: ast.Call reached as a statement expression, with-item, or decorator.
        """

        if (
            isinstance(node.func, ast.Name)
            and (finding := self.varying_functions.get(node.func.id))
            and finding not in self.findings
        ):
            self.findings.append(finding)
        if self.dag_detector.is_dag_constructor(node):
            self._check_and_record(node, VaryingContext.DAG_CONSTRUCTOR)
        elif self.dag_detector.is_task_constructor(node):
            self._check_and_record(node, VaryingContext.TASK_CONSTRUCTOR)

    def visit_For(self, node: ast.For) -> None:
        """Taint a loop target iterating varying values for the duration of the body.

        Parameters:
            node: ast.For to walk.
        """

        varying_source = self.value_analyzer.get_varying_source(node.iter)
        if varying_source and isinstance(node.target, ast.Name):
            self.varying_vars[node.target.id] = (node.lineno, varying_source)
        for statement in node.body:
            self.visit(statement)
        if varying_source and isinstance(node.target, ast.Name):
            # A nested loop over the same target already dropped the entry.
            self.varying_vars.pop(node.target.id, None)

    def visit_With(self, node: ast.With) -> None:
        """Enter a ``with DAG`` block's task context and register its ``as`` binding.

        Parameters:
            node: ast.With to walk.
        """

        is_with_dag_context = False
        for item in node.items:
            self.visit(item)
            if isinstance(item.context_expr, ast.Call) and self.dag_detector.is_dag_constructor(
                item.context_expr
            ):
                is_with_dag_context = True
                if isinstance(item.optional_vars, ast.Name):
                    self._register_dag_instances([item.optional_vars])
        if is_with_dag_context:
            self.dag_detector.is_in_dag_context = True
        for statement in node.body:
            self.visit(statement)
        if is_with_dag_context:
            self.dag_detector.is_in_dag_context = False

    def _visit_function_def(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        """Check task decorators, then walk the body as a deferred factory scope.

        A finding inside the body is parked in ``varying_functions`` and only surfaces
        when ``visit_Call`` later sees the function called by name.

        Parameters:
            node: ast.FunctionDef | ast.AsyncFunctionDef to walk.
        """

        for decorator in node.decorator_list:
            if self.dag_detector.is_task_decorator(decorator):
                if isinstance(decorator, ast.Call):
                    self._check_and_record(decorator, VaryingContext.TASK_DECORATOR)
                return
            self.visit(decorator)
        self.dag_detector.function_def_context = node.name
        for statement in node.body:
            self.visit(statement)
        self.dag_detector.function_def_context = None

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        """Walk one function definition as a deferred factory scope.

        Parameters:
            node: ast.FunctionDef to walk.
        """

        self._visit_function_def(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        """Walk one async function definition exactly like a sync one.

        Parameters:
            node: ast.AsyncFunctionDef to walk.
        """

        self._visit_function_def(node)

    def _register_dag_instances(self, targets: list[ast.expr]) -> None:
        """Register every plain-name assignment target as a Dag instance.

        Parameters:
            targets: list[ast.expr] containing the assignment or ``as`` targets.
        """

        for target in targets:
            if isinstance(target, ast.Name):
                self.dag_detector.register_dag_instance(target.id)

    def _track_varying_assignment(self, node: ast.Assign) -> None:
        """Taint every plain-name target of an assignment with a varying value.

        Parameters:
            node: ast.Assign whose value to inspect.
        """

        if varying_source := self.value_analyzer.get_varying_source(node.value):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    self.varying_vars[target.id] = (node.lineno, varying_source)

    def _check_and_record(self, call: ast.Call, context: VaryingContext) -> None:
        """Record one finding when a constructor call carries a varying argument.

        Parameters:
            call: ast.Call classified as a Dag/task constructor or task decorator.
            context: VaryingContext naming which constructor form the call is.
        """

        varying = self.value_analyzer.get_varying_argument(call)
        if varying is None:
            return
        if context is VaryingContext.TASK_CONSTRUCTOR and self.dag_detector.is_in_dag_context:
            context = VaryingContext.TASK_IN_DAG_BLOCK
        finding = RuntimeVaryingFinding(
            line=call.lineno,
            end_line=call.end_lineno or call.lineno,
            snippet=varying,
            context=context.value,
        )
        if self.dag_detector.function_def_context:
            self.varying_functions[self.dag_detector.function_def_context] = finding
        else:
            self.findings.append(finding)


def find_runtime_varying_dag_args(module: ast.Module, source: str) -> list[RuntimeVaryingFinding]:
    """Find every runtime-varying Dag or task constructor argument in one parsed Dag file.

    Parameters:
        module: ast.Module parsed from a Dag source file.
        source: str containing the full module source.

    Returns:
        list[RuntimeVaryingFinding] containing one finding per flagged constructor call,
        sorted by source line.
    """

    visitor = _RuntimeVaryingDagArgsVisitor(source)
    visitor.visit(module)
    return sorted(visitor.findings, key=lambda finding: (finding.line, finding.end_line))


__all__ = (
    "RUNTIME_VARYING_CALLS",
    "RuntimeVaryingFinding",
    "find_runtime_varying_dag_args",
)
