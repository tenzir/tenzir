import React, {useState} from 'react';
import {useKeenSlider} from 'keen-slider/react';
import clsx from 'clsx';
import styles from './styles.module.css';
import 'keen-slider/keen-slider.min.css';

const Slider = ({children}) => {
  const [currentSlide, setCurrentSlide] = React.useState(0);
  const [loaded, setLoaded] = useState(false);
  const [sliderRef, instanceRef] = useKeenSlider<HTMLDivElement>(
    {
      initial: 0,
      loop: true,

      slideChanged(slider) {
        setCurrentSlide(slider.track.details.rel);
      },
      created() {
        setLoaded(true);
      },
    },
    [
      (slider) => {
        let timeout: ReturnType<typeof setTimeout>;
        let mouseOver = false;
        function clearNextTimeout() {
          clearTimeout(timeout);
        }
        function nextTimeout() {
          clearTimeout(timeout);
          if (mouseOver) return;
          timeout = setTimeout(() => {
            slider.next();
          }, 2000);
        }
        slider.on('created', () => {
          slider.container.addEventListener('mouseover', () => {
            mouseOver = true;
            clearNextTimeout();
          });
          slider.container.addEventListener('mouseout', () => {
            mouseOver = false;
            nextTimeout();
          });
          nextTimeout();
        });
        slider.on('dragStarted', clearNextTimeout);
        slider.on('animationEnded', nextTimeout);
        slider.on('updated', nextTimeout);
      },
    ]
  );

  return (
    <>
      <div className="navigation-wrapper">
        <div ref={sliderRef} className="keen-slider">
          {children.map((child: React.ReactNode, index: number) => (
            <div key={index} className="keen-slider__slide">
              {child}
            </div>
          ))}
        </div>
      </div>
      {loaded && instanceRef.current && (
        <div className={styles.dots}>
          {[
            ...Array(instanceRef.current.track.details.slides.length).keys(),
          ].map((idx) => {
            return (
              <button
                key={idx}
                onClick={() => {
                  instanceRef.current?.moveToIdx(idx);
                }}
                className={clsx(
                  styles.dot,
                  currentSlide === idx ? styles.active : ''
                )}
              ></button>
            );
          })}
        </div>
      )}
    </>
  );
};

export default Slider;
