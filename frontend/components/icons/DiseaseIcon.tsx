const DiseaseIcon = (props: any) => {
  const { height = "1em", width = "1em", color = "black", ...rest } = props;

  return (
    <svg
      fill={color}
      height={height}
      width={width}
      xmlns="http://www.w3.org/2000/svg"
      viewBox="0 0 48 48"
      xmlSpace="preserve"
      {...rest}
    >
      <g data-name="Layer 2">
        <path fill="none" data-name="invisible box" d="M0 0H48V48H0z" />
        <path
          d="M44.5 22.1l-4.7-1.4a3.7 3.7 0 01-2.5-2.2 3.5 3.5 0 01.2-3.3l2.4-4.4a2.1 2.1 0 00-.3-2.4 2.1 2.1 0 00-2.4-.3l-4.4 2.4a3.5 3.5 0 01-3.3.2 3.7 3.7 0 01-2.2-2.5l-1.4-4.8a2 2 0 00-3.8 0l-1.4 4.8a3.7 3.7 0 01-2.2 2.5 3.5 3.5 0 01-3.3-.2l-4.4-2.4a2.1 2.1 0 00-2.4.3 2.1 2.1 0 00-.3 2.4l2.4 4.4a3.5 3.5 0 01.2 3.3 3.7 3.7 0 01-2.5 2.2l-4.7 1.4a2 2 0 000 3.8l4.7 1.4a3.7 3.7 0 012.5 2.2 3.5 3.5 0 01-.2 3.3l-2.4 4.4a2.1 2.1 0 00.3 2.4 2.1 2.1 0 002.4.3l4.4-2.4a3.5 3.5 0 013.3-.2 3.5 3.5 0 012.2 2.5l1.4 4.8a2 2 0 003.8 0l1.4-4.8a3.8 3.8 0 015.5-2.3l4.4 2.4a2 2 0 002.7-2.7l-2.4-4.4a3.5 3.5 0 01-.2-3.3 3.7 3.7 0 012.5-2.2l4.7-1.4a2 2 0 000-3.8zM21 25a4 4 0 114-4 4 4 0 01-4 4zm8 6a2 2 0 112-2 2 2 0 01-2 2z"
          data-name="Layer 4"
        />
      </g>
    </svg>
  );
};

DiseaseIcon.displayName = "DiseaseIcon";

export default DiseaseIcon;
