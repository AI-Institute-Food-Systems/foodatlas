const Graph = (props: any) => {
  const { height = "1em", width = "1em", color = "black", ...rest } = props;

  return (
    <svg
      width="1728"
      height="1117"
      viewBox="0 0 1728 1117"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
    >
      <g id="MacBook Pro 16&#34; - 1">
        <rect width="1728" height="1117" fill="black" />
        <line
          id="node_1_node_3"
          x1="752.16"
          y1="599.784"
          x2="648.16"
          y2="355.784"
          stroke="url(#paint0_linear_1297_1171)"
          stroke-width="4"
        />
        <circle id="node_3" cx="650.5" cy="355.5" r="17.5" fill="#FFF200" />
        <line
          id="node_1_node_2"
          x1="752.674"
          y1="597.503"
          x2="988.674"
          y2="388.503"
          stroke="url(#paint1_linear_1297_1171)"
          stroke-width="4"
        />
        <circle id="node_1" cx="754.5" cy="598.5" r="17.5" fill="#00A2FF" />
        <circle id="node_2" cx="989.5" cy="390.5" r="17.5" fill="#FF00EE" />
      </g>
      <defs>
        <linearGradient
          id="paint0_linear_1297_1171"
          x1="754.46"
          y1="598.804"
          x2="650.46"
          y2="354.804"
          gradientUnits="userSpaceOnUse"
        >
          <stop stop-color="#00A1FF" />
          <stop offset="1" stop-color="#FFF200" />
        </linearGradient>
        <linearGradient
          id="paint1_linear_1297_1171"
          x1="754.331"
          y1="599.374"
          x2="990.331"
          y2="390.374"
          gradientUnits="userSpaceOnUse"
        >
          <stop stop-color="#00A1FF" />
          <stop offset="1" stop-color="#FF00EE" />
        </linearGradient>
      </defs>
    </svg>
    // <svg
    //   fill={color}
    //   height={height}
    //   width={width}
    //   viewBox="0 -3.84 122.88 122.88"
    //   xmlns="http://www.w3.org/2000/svg"
    //   xmlSpace="preserve"
    //   {...rest}
    // >
    //   <path d="M29.03 100.46l20.79-25.21 9.51 12.13L41 110.69c-7.02 8.92-20.01-.48-11.97-10.23zm24.28-57.41c1.98-6.46 1.07-11.98-6.37-20.18L28.76 1c-2.58-3.03-8.66 1.42-6.12 5.09L37.18 24c2.75 3.34-2.36 7.76-5.2 4.32L16.94 9.8c-2.8-3.21-8.59 1.03-5.66 4.7 4.24 5.1 10.8 13.43 15.04 18.53 2.94 2.99-1.53 7.42-4.43 3.69L6.96 18.32c-2.19-2.38-5.77-.9-6.72 1.88-1.02 2.97 1.49 5.14 3.2 7.34L20.1 49.06c5.17 5.99 10.95 9.54 17.67 7.53 1.03-.31 2.29-.94 3.64-1.77l44.76 57.78c2.41 3.11 7.06 3.44 10.08.93l.69-.57c3.4-2.83 3.95-8 1.04-11.34l-47.4-54.46c1.38-1.54 2.39-3 2.73-4.11zm12.67 12.6l7.37-8.94C63.87 23.21 99-8.11 116.03 6.29 136.72 23.8 105.97 66 84.36 55.57l-8.73 11.09-9.65-11.01z" />
    // </svg>
  );
};

Graph.displayName = "Graph";

export default Graph;
