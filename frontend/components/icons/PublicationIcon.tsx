const PublicationIcon = (props: any) => {
  const { height = "1em", width = "1em", color = "black", ...rest } = props;

  return (
    <svg
      fill={color}
      height={height}
      width={width}
      xmlns="http://www.w3.org/2000/svg"
      fillRule="evenodd"
      clipRule="evenodd"
      imageRendering="optimizeQuality"
      shapeRendering="geometricPrecision"
      textRendering="geometricPrecision"
      viewBox="0 0 392 512.309"
      {...rest}
    >
      <path d="M58.883 0h186.242v93.425l.099 1.406c1.618 22.2 9.835 40.061 24.454 52.552 14.088 12.047 33.864 18.745 59.129 19.14l63.193.036v286.86c0 32.331-26.552 58.89-58.883 58.89H58.883C26.545 512.309 0 485.814 0 453.419V58.883C0 26.495 26.495 0 58.883 0zm48.892 412.172c-5.673 0-10.28-4.607-10.28-10.28 0-5.674 4.607-10.28 10.28-10.28h141.71c5.673 0 10.28 4.606 10.28 10.28 0 5.673-4.607 10.28-10.28 10.28h-141.71zm0-175.532c-5.673 0-10.28-4.607-10.28-10.28 0-5.674 4.607-10.28 10.28-10.28h176.45c5.673 0 10.28 4.606 10.28 10.28 0 5.673-4.607 10.28-10.28 10.28h-176.45zm0 84.77c-5.673 0-10.28-4.607-10.28-10.28 0-5.674 4.607-10.28 10.28-10.28h167.102c5.674 0 10.281 4.606 10.281 10.28 0 5.673-4.607 10.28-10.281 10.28H107.775zM265.685 3.159L392 139.816v6.197l-63.03.035c-20.179-.346-35.532-5.327-45.967-14.251-10.288-8.796-16.109-21.853-17.318-38.414V3.159z"></path>
    </svg>
  );
};

PublicationIcon.displayName = "PublicationIcon";

export default PublicationIcon;
