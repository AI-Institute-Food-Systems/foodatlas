const BioactivityIcon = (props: any) => {
  const { height = "1em", width = "1em", color = "black", ...rest } = props;

  return (
    <svg
      height={height}
      width={width}
      xmlns="http://www.w3.org/2000/svg"
      viewBox="0 0 48 48"
      xmlSpace="preserve"
      {...rest}
    >
      {/* L-shaped axes */}
      <path fill={color} d="M8 6h3v34h33v3H8z" />
      {/* sigmoid dose-response curve */}
      <path
        d="M12 37 C20 37 22 37 25 24 C28 11 30 11 42 11"
        fill="none"
        stroke={color}
        strokeWidth={4}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
};

BioactivityIcon.displayName = "BioactivityIcon";

export default BioactivityIcon;
