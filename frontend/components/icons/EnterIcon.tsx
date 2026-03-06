const EnterIcon = (props: any) => {
  const { height = "1em", width = "1em", color = "black", ...rest } = props;

  return (
    <svg
      width={width}
      height={height}
      viewBox="0 0 24 24"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      {...rest}
    >
      <path
        d="M20 7v1.2c0 1.68 0 2.52-.327 3.162a3 3 0 01-1.311 1.311C17.72 13 16.88 13 15.2 13H4m0 0l4-4m-4 4l4 4"
        stroke={color}
        strokeWidth={2}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
};

EnterIcon.displayName = "EnterIcon";

export default EnterIcon;
