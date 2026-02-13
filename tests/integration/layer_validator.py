import time
import asyncio
import numpy as np
import logging
# import statistics
from dataclasses import dataclass, field
from typing import Callable, Any, List, Dict

# Setup structured logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Pentecost.Validator")

@dataclass
class LayerValidationResult:
    layer_name: str
    passed: bool
    latency_p50: float
    latency_p95: float
    latency_p99: float
    throughput_qps: float
    jitter_ms: float
    errors: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)

class LayerValidator:
    def __init__(self, thresholds: Dict[str, Any]):
        self.thresholds = thresholds
        self.results: List[LayerValidationResult] = []

    async def validate_layer(
        self,
        layer_name: str,
        process_func: Callable,
        test_inputs: List[Any],
        concurrency: int = 5,
        warmup: int = 5
    ) -> LayerValidationResult:
        """
        Validated a layer under realistic concurrent load.
        """
        logger.info(f"--- Starting Validation: {layer_name} (Concurrency: {concurrency}) ---")
        
        # 1. Warmup (Sequential)
        for _ in range(warmup):
            await process_func(test_inputs[0])

        # 2. Parallel Measurement
        latencies = []
        errors = []
        
        start_time = time.perf_counter()
        
        # Process in batches of 'concurrency'
        for i in range(0, len(test_inputs), concurrency):
            batch = test_inputs[i:i + concurrency]
            tasks = [self._timed_run(process_func, item) for item in batch]
            
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for res in batch_results:
                if isinstance(res, Exception):
                    errors.append(str(res))
                else:
                    latencies.append(res)

        total_duration = time.perf_counter() - start_time
        
        # 3. Scientific Metrics Calculation
        if not latencies:
            return self._fail_result(layer_name, errors)

        lats = np.array(latencies)
        p50, p95, p99 = np.percentile(lats, [50, 95, 99])
        jitter = np.std(lats)
        qps = len(latencies) / total_duration

        # 4. SLA Verification
        sla = self.thresholds.get(layer_name, {})
        recs = []
        passed = True

        if p95 > sla.get('p95_ms', 10000):
            passed = False
            recs.append(f"Latency Spike: p95 ({p95:.1f}ms) exceeds SLA ({sla['p95_ms']}ms)")
        
        if jitter > (p50 * 0.5): # Jitter > 50% of p50 suggests instability
            recs.append(f"High Jitter: {jitter:.2f}ms. Check for resource contention or GC.")

        result = LayerValidationResult(
            layer_name=layer_name,
            passed=passed,
            latency_p50=p50,
            latency_p95=p95,
            latency_p99=p99,
            throughput_qps=qps,
            jitter_ms=jitter,
            errors=errors,
            recommendations=recs
        )
        
        self.results.append(result)
        self._log_summary(result)
        return result

    async def _timed_run(self, func: Callable, input_data: Any) -> float:
        start = time.perf_counter()
        await func(input_data)
        return (time.perf_counter() - start) * 1000

    def _fail_result(self, name: str, errors: list) -> LayerValidationResult:
        return LayerValidationResult(name, False, 0, 0, 0, 0, 0, errors, ["Critical Failure: No successful runs"])

    def _log_summary(self, r: LayerValidationResult):
        status = "✅" if r.passed else "❌"
        logger.info(f"{status} {r.layer_name} | p95: {r.latency_p95:.1f}ms | QPS: {r.throughput_qps:.1f}")

    def generate_markdown_report(self) -> str:
        """Outputs a clean table for the docs/performance/ folder."""
        lines = ["# Pentecost Engine v2.0 Performance Report\n", "| Layer | Status | p95 Latency | QPS | Jitter |", "| :--- | :--- | :--- | :--- | :--- |"]
        for r in self.results:
            status = "PASS" if r.passed else "FAIL"
            lines.append(f"| {r.layer_name} | {status} | {r.latency_p95:.1f}ms | {r.throughput_qps:.1f} | {r.jitter_ms:.1f}ms |")
        return "\n".join(lines)