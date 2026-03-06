import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

describe("smoke test", () => {
  it("renders a simple element", () => {
    render(<h1>FoodAtlas</h1>);
    expect(screen.getByText("FoodAtlas")).toBeInTheDocument();
  });
});
