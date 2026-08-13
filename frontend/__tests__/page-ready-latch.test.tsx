import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import {
  PageReadyProvider,
  useLoadingGate,
  usePageReady,
} from "@/context/pageReadyContext";

const Loader = ({ loading }: { loading: boolean }) => {
  useLoadingGate(loading);
  return null;
};

const ReadyProbe = () => {
  const { ready } = usePageReady();
  return <div data-testid="ready">{ready ? "yes" : "no"}</div>;
};

const Harness = ({ children }: { children: React.ReactNode }) => (
  <PageReadyProvider>
    <ReadyProbe />
    {children}
  </PageReadyProvider>
);

const readyText = () => screen.getByTestId("ready").textContent;

describe("PageReadyProvider", () => {
  it("holds the gate closed until the first loader completes", () => {
    const { rerender } = render(
      <Harness>
        <Loader loading />
      </Harness>
    );
    expect(readyText()).toBe("no");

    rerender(
      <Harness>
        <Loader loading={false} />
      </Harness>
    );
    expect(readyText()).toBe("yes");
  });

  it("stays ready when late content registers a new loader", () => {
    const { rerender } = render(
      <Harness>
        <Loader loading={false} />
      </Harness>
    );
    expect(readyText()).toBe("yes");

    // A tab opened for the first time mounts sections that register their
    // own loaders. Before the latch this pushed `registered` past
    // `completed` and dropped the whole page back to a skeleton.
    rerender(
      <Harness>
        <Loader loading={false} />
        <Loader loading />
      </Harness>
    );
    expect(readyText()).toBe("yes");
  });
});
