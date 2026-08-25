<!-- DEFENSIVE_VERIFICATION_START -->
# Engineering Verification Rules
1. **Verification Status Report Required**: When presenting completed code changes or bug fixes, you must append a structured `Verification Status Report` (`[Automated Tests: Executed/Skipped]`, `[Type & Static Checks: Tool-Checked/Manual Review]`, and `[Manual Verification Required]`).
2. **No Hallucinated Test Execution**: Never claim tests passed if no command was executed. If no automated tests exist in the project, explicitly state that verification was degraded to manual static inspection of boundary conditions.
3. **Two-Stage Rule Learning**: Never modify project or global rule files autonomously. Only propose structured 7-field candidate lessons when prompted or after solving complex bugs, and wait for explicit user confirmation before persisting.
4. **Strict Contract & Signature Isolation**:
   - **No Runtime Signature Guessing**: Never use `try...except (TypeError, ValueError)` to guess callable signatures or adapt parameters at runtime. Inspect signatures statically via `inspect.signature` or `typing.get_type_hints` outside the actual invocation block so business `TypeError`s are not caught and retried.
   - **Strict Fake Testing**: Never use permissive `**kwargs` in test fakes/mocks for third-party SDKs (e.g. OpenAI, Anthropic). Test fakes must mirror exact official signatures to catch illegal parameter forwarding early.
   - **Sole Retry Ownership**: When wrapping third-party SDK clients with outer retry orchestration, disable client-level retries via `with_options(max_retries=0)` at the client level without mutating shared client instances or leaking `max_retries` into request methods.
<!-- DEFENSIVE_VERIFICATION_END -->
